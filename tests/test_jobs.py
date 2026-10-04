import asyncio
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from jobkit import CronSchedule, Job, Worker, next_cron
from jobkit.postgres import PostgresStore
from jobkit.redis import RedisStore


@pytest.fixture(params=["postgres", "redis"])
def store(request):
    if request.param == "postgres":
        dsn = os.getenv("JOBKIT_POSTGRES")
        if not dsn:
            pytest.skip("set JOBKIT_POSTGRES for real PostgreSQL tests")
        result = PostgresStore(dsn)
        result.initialize()
    else:
        url = os.getenv("JOBKIT_REDIS")
        if not url:
            pytest.skip("set JOBKIT_REDIS for real Redis tests")
        result = RedisStore(url, "test-jobkit-" + uuid.uuid4().hex)
    ids = []

    class Tracked:
        def enqueue(self, job):
            ident = result.enqueue(job)
            ids.append(ident)
            return ident

        def __getattr__(self, key):
            return getattr(result, key)

    yield Tracked()
    # Test database is dedicated. Terminate only jobs this fixture inserted.
    if isinstance(result, RedisStore):
        result.client.delete(*result.keys)
        result.client.close()
    else:
        import psycopg

        with psycopg.connect(dsn) as conn:
            for ident in ids:
                conn.execute("DELETE FROM jobkit_jobs WHERE id=%s", (ident,))


def test_unique_concurrent_claim_and_payload(store):
    original = Job.create(
        "work", {"list": [], "big": 9007199254740993}, unique_key="key"
    )
    ident = store.enqueue(original)
    assert store.enqueue(Job.create("work", None, unique_key="key")) == ident
    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(executor.map(lambda _: store.claim(5), range(8)))
    jobs = [j for j in claims if j]
    assert len(jobs) == 1
    job = jobs[0]
    assert job.payload == original.payload
    assert not store.purge(ident)
    assert store.finish(job, None, 0)
    assert not store.finish(job, None, 0)
    assert store.enqueue(Job.create("work", None, unique_key="key")) == ident
    assert store.purge(ident)
    assert store.enqueue(Job.create("work", None, unique_key="key")) != ident
    next_job = store.claim(5)
    assert store.finish(next_job, None, 0)


def test_retry_dead_and_delayed(store):
    ident = store.enqueue(Job.create("work", None, max_attempts=2, delay=0.08))
    assert store.claim(2) is None
    time.sleep(0.09)
    job = store.claim(2)
    assert job.attempts == 1
    assert store.finish(job, "broken", 0.07)
    assert store.claim(2) is None
    time.sleep(0.08)
    retry = store.claim(2)
    assert retry.attempts == 2
    assert store.finish(retry, "again", 0)
    assert store.get(ident).status == "dead"
    assert ident in [j.id for j in store.dead_jobs()]
    assert store.claim(2) is None


def test_expired_lease_fences_stale_worker(store):
    ident = store.enqueue(Job.create("work", None, max_attempts=2))
    first = store.claim(0.04)
    time.sleep(0.05)
    assert not store.finish(first, None, 0)
    second = store.claim(0.04)
    assert second.token != first.token and second.attempts == 2
    assert not store.finish(first, None, 0)
    time.sleep(0.05)
    assert store.claim(1) is None
    assert store.get(ident).status == "dead"


async def test_worker_bounded_and_graceful(store):
    stop = asyncio.Event()
    active, maximum, complete = 0, 0, 0

    async def handler(job):
        nonlocal active, maximum, complete
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.03)
        active -= 1
        complete += 1
        if complete == 5:
            stop.set()

    ids = [store.enqueue(Job.create("work", {"i": i})) for i in range(5)]
    await asyncio.wait_for(Worker(store, {"work": handler}, concurrency=2).run(stop), 5)
    assert maximum == 2 and active == 0
    assert all(store.get(ident).status == "done" for ident in ids)


async def test_timeout_and_missing_handler_are_dead(store):
    stop = asyncio.Event()

    async def slow(job):
        await asyncio.sleep(10)

    ids = [
        store.enqueue(Job.create(name, None, max_attempts=1))
        for name in ("slow", "missing")
    ]
    task = asyncio.create_task(
        Worker(store, {"slow": slow}, timeout=0.02, lease_seconds=1).run(stop)
    )
    for _ in range(100):
        if all(store.get(ident).status == "dead" for ident in ids):
            break
        await asyncio.sleep(0.02)
    stop.set()
    await task
    assert "TimeoutError" in store.get(ids[0]).error
    assert "KeyError" in store.get(ids[1]).error


@pytest.mark.parametrize("payload", [float("nan"), object(), {"x": float("inf")}])
def test_unsafe_payload_rejected(payload):
    with pytest.raises((ValueError, TypeError)):
        Job.create("work", payload)


def test_cron_and_configuration():
    after = datetime(2026, 1, 1, tzinfo=UTC)
    assert next_cron("0 9 * * *", after, "America/Sao_Paulo").hour == 12
    with pytest.raises(ValueError):
        next_cron("* * * * *", datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        Worker(None, {}, timeout=60, lease_seconds=30)
    with pytest.raises(ValueError):
        Job.create("", None)


def test_retry_cap_and_dst():
    worker = Worker(None, {}, retry_delay=1, max_retry_delay=30)
    assert worker.retry_seconds(1) == 1
    assert worker.retry_seconds(6) == 30
    assert worker.retry_seconds(1000000) == 30
    assert Worker(None, {}, retry_delay=1e308).retry_seconds(2) == 300
    spring = datetime.fromisoformat("2026-03-08T00:00:00-05:00")
    assert next_cron("30 2 * * *", spring, "America/New_York") == (
        datetime.fromisoformat("2026-03-08T07:00:00+00:00")
    )
    fall = datetime.fromisoformat("2026-11-01T01:31:00-04:00")
    assert next_cron("30 1 * * *", fall, "America/New_York") == (
        datetime.fromisoformat("2026-11-01T06:30:00+00:00")
    )


def test_global_limit_and_invalid_parameters(store):
    ids = [store.enqueue(Job.create("work", {"nested": [{}, []]})) for _ in range(6)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(executor.map(lambda _: store.claim(3), range(8)))
    leased = [job for job in claims if job]
    assert len(leased) == 4
    assert all(job.payload == {"nested": [{}, []]} for job in leased)
    for job in leased:
        assert store.finish(job, None, 0)
    for _ in range(2):
        assert store.finish(store.claim(3), None, 0)
    assert all(store.get(ident).status == "done" for ident in ids)
    for invalid in (float("nan"), float("inf"), -1):
        with pytest.raises(ValueError):
            store.claim(invalid)
        with pytest.raises(ValueError):
            store.finish(leased[0], None, invalid)


async def test_two_workers_share_global_limit(store):
    stop = asyncio.Event()
    active, maximum, done = 0, 0, 0

    async def work(job):
        nonlocal active, maximum, done
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.04)
        active -= 1
        done += 1
        if done == 12:
            stop.set()

    ids = [store.enqueue(Job.create("work", None)) for _ in range(12)]
    workers = [Worker(store, {"work": work}, concurrency=4) for _ in range(2)]
    await asyncio.wait_for(asyncio.gather(*(w.run(stop) for w in workers)), 5)
    assert maximum <= 4 and done == 12
    assert all(store.get(ident).status == "done" for ident in ids)


def test_recurring_scheduler_catchup_restart_and_dedup(store):
    start = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    end = datetime.fromisoformat("2026-01-01T00:03:00+00:00")
    schedule = CronSchedule(store, "digest", "* * * * *", "work", None, after=start)
    first = schedule.tick(end, max_occurrences=2)
    second = schedule.tick(end)
    assert len(first) == 2 and len(second) == 1
    assert schedule.tick(end) == []
    # A second process/restart with an explicit cursor finds existing occurrences.
    replay = CronSchedule(store, "digest", "* * * * *", "work", None, after=start)
    assert replay.tick(end) == first + second
    for _ in range(3):
        assert store.finish(store.claim(5), None, 0)
