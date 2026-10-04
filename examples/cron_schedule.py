"""Application-owned recurring schedule with explicit restart cursor."""

import os
from datetime import UTC, datetime

from jobkit import CronSchedule
from jobkit.redis import RedisStore

store = RedisStore(os.environ.get("JOBKIT_REDIS", "redis://localhost:6379/0"))
schedule = CronSchedule(
    store, "weekday-digest", "0 9 * * 1-5", "digest", None, zone="America/Sao_Paulo"
)
print(schedule.tick(datetime.now(UTC)))
# In your async application: await schedule.run(stop_event)
# Persist schedule.cursor if bounded catch-up after restart is desired.
store.client.close()
