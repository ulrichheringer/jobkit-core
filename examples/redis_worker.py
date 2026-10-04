import asyncio
import os
import uuid

from jobkit_core import Job, Worker
from jobkit_core.redis import RedisStore


async def main() -> None:
    store = RedisStore(os.environ.get("JOBKIT_REDIS", "redis://localhost:6379/0"))
    stop = asyncio.Event()

    async def greet(job: Job) -> None:
        print("Hello", job.payload)
        stop.set()

    ident = store.enqueue(Job.create("greet", "world", unique_key=uuid.uuid4().hex))
    await Worker(store, {"greet": greet}, concurrency=1).run(stop)
    assert store.get(ident).status == "done"
    store.purge(ident)
    store.client.close()


if __name__ == "__main__":
    asyncio.run(main())
