# jobkit-core

Small, typed background jobs for Python 3.11+ with real PostgreSQL and Redis storage. Jobs support delays, cron scheduling, retries, uniqueness, bounded execution and dead-job inspection. MIT licensed.

The original `jobkit` name was already used on PyPI and GitHub; this distribution uses `jobkit-core` and imports as `jobkit`.

## Installation

```sh
pip install 'jobkit-core[postgres]' # or jobkit-core[redis]
```

No application containers are needed. PostgreSQL 14+ or Redis 6+ are external infrastructure. The base dependency is croniter; backend drivers are optional.

## Quick start

```python
import asyncio
from jobkit import Job, Worker
from jobkit.postgres import PostgresStore

store = PostgresStore(
    "postgresql://postgres:secret@localhost/jobs", concurrency_limit=4
)
store.initialize()
store.enqueue(Job.create("email", {"user_id": 42}, unique_key="welcome:42"))


async def email(job: Job) -> None:
    print(job.payload, job.attempts, job.token)


async def main() -> None:
    stop = asyncio.Event()
    # Set stop from your application shutdown hook.
    await Worker(store, {"email": email}, concurrency=2).run(stop)


asyncio.run(main())
```

For Redis use `RedisStore('redis://localhost:6379/0', namespace='emails', concurrency_limit=4)` from `jobkit.redis`. No initialization is needed. Use a dedicated namespace, configure AOF/RDB and replication according to your durability requirements, and prevent eviction of job keys. Redis operations use Lua and both keys share a cluster hash slot.

## Scheduling

`Job.create(..., delay=30)` schedules relative to the producer clock. `at=` accepts an aware datetime and cannot accompany delay. Synchronize producer clocks. Claims and lease expiry use the database server clock.

```python
from datetime import datetime, timezone
from jobkit import next_cron

at = next_cron("0 9 * * 1-5", datetime.now(timezone.utc), "America/Sao_Paulo")
store.enqueue(Job.create("digest", None, at=at, unique_key=f"digest:{at.isoformat()}"))
```

`next_cron` accepts five-field cron syntax and an IANA timezone; the result is UTC. Cron follows croniter 6 wall-clock rules, including its DST transitions: a New York 02:30 occurrence on the 2026 spring-forward date moves to 03:00, and 01:30 on fall-back can occur in both folds. These cases are regression-tested. Use UTC for a fixed cadence. Use `CronSchedule(store, schedule_id, expression, name, payload, zone="UTC", after=cursor)` for recurring jobs. `tick(until, max_occurrences=100)` atomically enqueues each occurrence in `(cursor, until]` and advances the in-memory cursor after successful enqueue. Run continuously with `await schedule.run(stop_event)`. Deterministic uniqueness keys deduplicate concurrent schedulers. Without `after`, restart begins at the current time and skips missed jobs. Persist `schedule.cursor` in your application to enable bounded catch-up after restart; each tick enqueues at most its batch limit. Keep terminal jobs until all scheduler cursors have passed them, since purging releases deduplication keys.

## Execution guarantees

Claims atomically create an opaque ownership token and increment attempts. PostgreSQL uses transaction locks and `SKIP LOCKED`; Redis uses one atomic Lua script. Each store has a global concurrency limit (default 4). All processes sharing its table/namespace must configure the same limit. Each Worker independently bounds its own lanes with `concurrency`.

Handlers must be cooperative async functions: synchronous blocking work must move to a thread. `timeout` must be shorter than `lease_seconds`. Stop drains current handlers; cancelling the worker leaves claims to expire. Leases have no heartbeat. Expired leases can be reclaimed with a new token; an expired or stale worker cannot finish a job. A final expired attempt becomes dead. A failed storage operation propagates and stops the worker; restart it under your application supervisor. A rejected finish logs a `jobkit` warning with the job ID.

Delivery can repeat after crashes, timeouts and expired leases. This does **not** provide exactly-once external effects. Make handlers idempotent and enforce fencing/idempotency in the destination when stale workers could produce effects. `job.token` identifies the ownership claim; `job.attempts` is its increasing per-job attempt number.

Errors, including missing handlers, retry with exponential delay capped at `max_retry_delay` (300 seconds by default). `max_attempts` counts claims, including claims abandoned by crashes. Exhausted jobs enter `dead`. Inspect with `store.dead_jobs()` and `store.get(id)`; enqueue a new job to retry manually.

## Uniqueness and retention

An optional `unique_key` atomically returns the existing job ID throughout queued, running, done and dead states. Terminal jobs remain stored for inspection and retain their keys. `store.purge(id)` removes only a terminal job and releases its key. There is no automatic TTL or result storage; handlers return None. Use bounded retention in an application maintenance task. Payloads are JSON, copied on creation, and preserve empty arrays and large integers in Redis. No pickle or arbitrary code deserialization is used.

PostgreSQL uses a dedicated `jobkit_jobs` table in the DSN's search path. Use separate databases/schemas for isolated queues. Redis scans one namespace hash inside Lua; this adapter intentionally targets **small queues**, since large hashes block Redis. PostgreSQL is the recommended backend for larger workloads. Connections open per PostgreSQL operation; deployment pooling can use PgBouncer.

## Development and validation

```sh
python -m venv .venv
# Activate your environment, then:
python -m pip install -e '.[dev]'
export JOBKIT_POSTGRES='postgresql://postgres:secret@localhost/jobs'
export JOBKIT_REDIS='redis://localhost:6379/0'
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m build
python examples/redis_worker.py
```

PowerShell uses `$env:JOBKIT_POSTGRES='...'` and `$env:JOBKIT_REDIS='...'`. Tests use real backend operations and concurrent claims; without backend URLs those integration cases are explicitly skipped. Use a dedicated test database, not production. Test cleanup deletes only fixture jobs and private Redis namespace keys. GitHub Actions supplies PostgreSQL and Redis service containers and tests Python 3.11 and 3.14. Applications themselves run natively.

See `examples/redis_worker.py` for a runnable worker and `examples/cron_schedule.py` for an explicit recurring scheduler.

