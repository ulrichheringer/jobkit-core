"""PostgreSQL adapter using short transactions and SKIP LOCKED claims."""

from __future__ import annotations

import math
import uuid

import psycopg

from .core import Job, decode, encode, validate_new


class PostgresStore:
    """Uses a dedicated jobkit_jobs table; connections open per operation."""

    def __init__(self, dsn: str, *, concurrency_limit: int = 4) -> None:
        if concurrency_limit < 1:
            raise ValueError("concurrency_limit must be positive")
        self.dsn = dsn
        self.limit = concurrency_limit

    def initialize(self) -> None:
        """Create the table explicitly (not implicitly on import)."""
        with psycopg.connect(self.dsn) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS jobkit_jobs (
                id text PRIMARY KEY, unique_key text UNIQUE,
                status text NOT NULL, due double precision NOT NULL,
                lease_until double precision NOT NULL DEFAULT 0,
                token text NOT NULL DEFAULT '', data text NOT NULL)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS jobkit_jobs_due
                ON jobkit_jobs(status, due)""")

    def enqueue(self, job: Job) -> str:
        validate_new(job)
        with psycopg.connect(self.dsn) as conn:
            result = conn.execute(
                """INSERT INTO jobkit_jobs(id,unique_key,status,due,data)
                VALUES (%s,%s,'queued',%s,%s) ON CONFLICT DO NOTHING RETURNING id""",
                (job.id, job.unique_key, job.due, encode(job)),
            ).fetchone()
            if result:
                return str(result[0])
            existing = conn.execute(
                "SELECT id FROM jobkit_jobs WHERE id=%s OR unique_key=%s",
                (job.id, job.unique_key),
            ).fetchone()
            if existing is None:
                raise RuntimeError(
                    "conflicting job was concurrently purged; retry enqueue"
                )
            return str(existing[0])

    def claim(self, lease_seconds: float) -> Job | None:
        if lease_seconds <= 0 or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be positive")
        with psycopg.connect(self.dsn) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(1874459012)")
            now_row = conn.execute(
                "SELECT extract(epoch from clock_timestamp())"
            ).fetchone()
            assert now_row
            now = float(now_row[0])
            count = conn.execute(
                "SELECT count(*) FROM jobkit_jobs "
                "WHERE status='running' AND lease_until>%s",
                (now,),
            ).fetchone()
            assert count
            if count[0] >= self.limit:
                return None
            while True:
                row = conn.execute(
                    """SELECT data FROM jobkit_jobs WHERE
                    (status='queued' AND due<=%s) OR
                    (status='running' AND lease_until<=%s)
                    ORDER BY due,id FOR UPDATE SKIP LOCKED LIMIT 1""",
                    (now, now),
                ).fetchone()
                if row is None:
                    return None
                old = decode(row[0])
                fields = dict(old.__dict__)
                if old.attempts >= old.max_attempts:
                    fields.update(
                        status="dead", error="lease expired after final attempt"
                    )
                else:
                    fields.update(
                        status="running",
                        attempts=old.attempts + 1,
                        token=str(uuid.uuid4()),
                        lease_until=now + lease_seconds,
                    )
                job = Job(**fields)
                conn.execute(
                    """UPDATE jobkit_jobs SET status=%s,lease_until=%s,token=%s,data=%s
                    WHERE id=%s""",
                    (job.status, job.lease_until, job.token, encode(job), job.id),
                )
                if job.status == "running":
                    return job

    def finish(self, job: Job, error: str | None, retry_delay: float) -> bool:
        if retry_delay < 0 or not math.isfinite(retry_delay):
            raise ValueError("retry_delay must be nonnegative and finite")
        with psycopg.connect(self.dsn) as conn:
            row = conn.execute(
                """SELECT data,extract(epoch from clock_timestamp()) FROM jobkit_jobs
                WHERE id=%s AND token=%s AND status='running'
                AND lease_until>extract(epoch from clock_timestamp()) FOR UPDATE""",
                (job.id, job.token),
            ).fetchone()
            if row is None:
                return False
            current = decode(row[0])
            fields = dict(current.__dict__)
            status = (
                "done"
                if error is None
                else ("dead" if current.attempts >= current.max_attempts else "queued")
            )
            fields.update(
                status=status,
                error=error,
                due=float(row[1]) + retry_delay,
                lease_until=0,
                token="",
            )
            updated = Job(**fields)
            conn.execute(
                """UPDATE jobkit_jobs SET status=%s,due=%s,
                lease_until=0,token='',data=%s
                WHERE id=%s""",
                (status, updated.due, encode(updated), job.id),
            )
            return True

    def get(self, job_id: str) -> Job | None:
        with psycopg.connect(self.dsn) as conn:
            row = conn.execute(
                "SELECT data FROM jobkit_jobs WHERE id=%s", (job_id,)
            ).fetchone()
            return decode(row[0]) if row else None

    def dead_jobs(self) -> list[Job]:
        with psycopg.connect(self.dsn) as conn:
            return [
                decode(r[0])
                for r in conn.execute(
                    "SELECT data FROM jobkit_jobs WHERE status='dead' ORDER BY due,id"
                )
            ]

    def purge(self, job_id: str) -> bool:
        with psycopg.connect(self.dsn) as conn:
            return (
                conn.execute(
                    """DELETE FROM jobkit_jobs WHERE id=%s
                AND status IN ('done','dead')""",
                    (job_id,),
                ).rowcount
                == 1
            )
