"""Explicit cron scheduler with bounded catch-up and occurrence deduplication."""

from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime

from .core import JSON, Job, Store, next_cron


class CronSchedule:
    """One recurring schedule. Cursor belongs to the application, not the store."""

    def __init__(
        self,
        store: Store,
        schedule_id: str,
        expression: str,
        name: str,
        payload: JSON,
        *,
        zone: str = "UTC",
        after: datetime | None = None,
        max_attempts: int = 3,
    ) -> None:
        if not schedule_id:
            raise ValueError("schedule_id must not be empty")
        self.cursor = after or datetime.now(UTC)
        next_cron(expression, self.cursor, zone)  # Validate eagerly.
        self.template = Job.create(name, payload, max_attempts=max_attempts)
        self.store, self.id, self.expression = store, schedule_id, expression
        self.zone = zone

    def tick(self, until: datetime, *, max_occurrences: int = 100) -> list[str]:
        """Enqueue occurrences in (cursor, until], then advance after each success."""
        if until.tzinfo is None or until < self.cursor or max_occurrences < 1:
            raise ValueError("use an aware forward time and a positive batch limit")
        ids = []
        for _ in range(max_occurrences):
            at = next_cron(self.expression, self.cursor, self.zone)
            if at > until:
                break
            ids.append(
                self.store.enqueue(
                    Job.create(
                        self.template.name,
                        self.template.payload,
                        at=at,
                        max_attempts=self.template.max_attempts,
                        unique_key=f"cron:{len(self.id)}:{self.id}:{at.isoformat()}",
                    )
                )
            )
            self.cursor = at
        return ids

    async def run(self, stop: asyncio.Event, *, interval: float = 1) -> None:
        """Poll with bounded batches; stop after any in-progress tick completes."""
        if interval <= 0 or not math.isfinite(interval):
            raise ValueError("interval must be positive and finite")
        while not stop.is_set():
            await asyncio.to_thread(self.tick, datetime.now(UTC))
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass
