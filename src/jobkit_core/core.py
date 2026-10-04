"""Durable job data and a bounded asynchronous worker."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Protocol, TypeAlias
from zoneinfo import ZoneInfo

from croniter import croniter

JSON: TypeAlias = None | bool | int | float | str | list["JSON"] | dict[str, "JSON"]


@dataclass(frozen=True)
class Job:
    """A snapshot. Token and attempt identify one leased execution."""

    id: str
    name: str
    payload: JSON
    due: float
    max_attempts: int = 3
    attempts: int = 0
    status: str = "queued"
    token: str = ""
    lease_until: float = 0
    unique_key: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.id
            or not self.name
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= 2147483647
        ):
            raise ValueError("job ID, name and max_attempts must be valid")
        if not math.isfinite(self.due) or not math.isfinite(self.lease_until):
            raise ValueError("job timestamps must be finite")
        if self.attempts < 0 or self.attempts > self.max_attempts:
            raise ValueError("attempts must be within max_attempts")
        json.dumps(self.payload, allow_nan=False)

    @classmethod
    def create(
        cls,
        name: str,
        payload: JSON,
        *,
        delay: float = 0,
        at: datetime | None = None,
        max_attempts: int = 3,
        unique_key: str | None = None,
    ) -> Job:
        """Create a JSON-safe job, optionally delayed or scheduled at an aware time."""
        if not name or max_attempts < 1 or delay < 0 or not math.isfinite(delay):
            raise ValueError("name, max_attempts and delay must be valid")
        if at is not None and (at.tzinfo is None or delay):
            raise ValueError("at must be timezone aware and cannot accompany delay")
        # Roundtrip isolates caller mutations and rejects NaN/infinity/pickle objects.
        safe = json.loads(json.dumps(payload, allow_nan=False))
        due = at.timestamp() if at else (time.time() + delay if delay else 0)
        if not math.isfinite(due):
            raise ValueError("due must be finite")
        return cls(
            str(uuid.uuid4()), name, safe, due, max_attempts, unique_key=unique_key
        )


class Store(Protocol):
    """Synchronous atomic storage operations; workers run these in threads."""

    def enqueue(self, job: Job) -> str: ...
    def claim(self, lease_seconds: float) -> Job | None: ...
    def finish(self, job: Job, error: str | None, retry_delay: float) -> bool: ...
    def get(self, job_id: str) -> Job | None: ...
    def dead_jobs(self) -> list[Job]: ...
    def purge(self, job_id: str) -> bool: ...


def encode(job: Job) -> str:
    fields = asdict(job)
    fields["payload"] = json.dumps(job.payload, allow_nan=False)
    return json.dumps(fields, allow_nan=False)


def decode(raw: str) -> Job:
    fields = json.loads(raw)
    fields["payload"] = json.loads(fields["payload"])
    return Job(**fields)


def validate_new(job: Job) -> None:
    if job.status != "queued" or job.attempts or job.token or job.lease_until:
        raise ValueError("enqueue accepts only fresh queued jobs")


def next_cron(expression: str, after: datetime, zone: str = "UTC") -> datetime:
    """Next five-field wall-clock cron occurrence, returned in UTC."""
    if after.tzinfo is None or len(expression.split()) != 5:
        raise ValueError("use an aware datetime and a five-field cron expression")
    local = after.astimezone(ZoneInfo(zone))
    result: datetime = croniter(expression, local).get_next(datetime)
    return result.astimezone(UTC)


Handler: TypeAlias = Callable[[Job], Awaitable[None]]


class Worker:
    """Bounded polling worker. Stop drains handlers; cancellation leaves leases."""

    def __init__(
        self,
        store: Store,
        handlers: Mapping[str, Handler],
        *,
        concurrency: int = 4,
        lease_seconds: float = 60,
        timeout: float = 30,
        retry_delay: float = 1,
        max_retry_delay: float = 300,
        poll_interval: float = 0.1,
    ) -> None:
        values = (lease_seconds, timeout, retry_delay, poll_interval, max_retry_delay)
        if concurrency < 1 or any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("worker limits must be positive and finite")
        if timeout >= lease_seconds:
            raise ValueError("timeout must be less than lease_seconds")
        self.store, self.handlers = store, dict(handlers)
        self.concurrency, self.lease = concurrency, lease_seconds
        self.timeout, self.retry, self.poll = timeout, retry_delay, poll_interval
        self.max_retry = max_retry_delay

    def retry_seconds(self, attempt: int) -> float:
        """Overflow-safe exponential backoff capped at max_retry_delay."""
        exponent = max(0, attempt - 1)
        if self.retry >= self.max_retry or exponent >= 1024:
            return self.max_retry
        if exponent >= math.log2(self.max_retry) - math.log2(self.retry):
            return self.max_retry
        return min(self.max_retry, math.ldexp(self.retry, exponent))

    async def run(self, stop: asyncio.Event) -> None:
        """Run until stop is set, then wait for the current bounded handlers."""

        async def lane() -> None:
            while not stop.is_set():
                job = await asyncio.to_thread(self.store.claim, self.lease)
                if job is None:
                    try:
                        await asyncio.wait_for(stop.wait(), self.poll)
                    except TimeoutError:
                        pass
                    continue
                error = None
                try:
                    async with asyncio.timeout(self.timeout):
                        await self.handlers[job.name](job)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"[:2000]
                finished = await asyncio.to_thread(
                    self.store.finish, job, error, self.retry_seconds(job.attempts)
                )
                if not finished:
                    logging.getLogger("jobkit").warning("lease lost for job %s", job.id)

        async with asyncio.TaskGroup() as group:
            for _ in range(self.concurrency):
                group.create_task(lane())
