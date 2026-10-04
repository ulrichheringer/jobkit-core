"""Redis Lua adapter. Each namespace is atomic and fits in one cluster slot."""

from __future__ import annotations

import math
import uuid

from redis import Redis

from .core import Job, decode, encode, validate_new

# A single hash is deliberately bounded in scope: small queues, no Lua key scans.
_SCRIPT = """
local op = ARGV[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
if op == 'enqueue' then
  local j = cjson.decode(ARGV[2])
  if redis.call('HEXISTS', KEYS[1],j.id)==1 then return j.id end
  if j.unique_key ~= cjson.null then
    local id=redis.call('HGET',KEYS[2],j.unique_key)
    if id then return id end
    redis.call('HSET',KEYS[2],j.unique_key,j.id)
  end
  redis.call('HSET',KEYS[1],j.id,ARGV[2]); return j.id
elseif op == 'claim' then
  local rows=redis.call('HGETALL',KEYS[1])
  local best=nil
  local running=0
  for i=1,#rows,2 do
    local j=cjson.decode(rows[i+1])
    if j.status=='running' and j.lease_until>now then running=running+1 end
    if (j.status=='queued' and j.due<=now) or
       (j.status=='running' and j.lease_until<=now) then
      if j.attempts>=j.max_attempts then
        j.status='dead'; j.error='lease expired after final attempt'
        redis.call('HSET',KEYS[1],j.id,cjson.encode(j))
      elseif not best or j.due<best.due or (j.due==best.due and j.id<best.id) then
        best=j
      end
    end
  end
  if not best or running>=tonumber(ARGV[4]) then return nil end
  best.status='running'; best.attempts=best.attempts+1
  best.token=ARGV[3]; best.lease_until=now+tonumber(ARGV[2])
  local raw=cjson.encode(best); redis.call('HSET',KEYS[1],best.id,raw); return raw
elseif op=='finish' or op=='purge' then
  local raw=redis.call('HGET',KEYS[1],ARGV[2]); if not raw then return 0 end
  local j=cjson.decode(raw)
  if op=='purge' then
    if j.status~='done' and j.status~='dead' then return 0 end
    redis.call('HDEL',KEYS[1],j.id)
    if j.unique_key~=cjson.null then redis.call('HDEL',KEYS[2],j.unique_key) end
    return 1
  end
  if j.status~='running' or j.token~=ARGV[3] or j.lease_until<=now then return 0 end
  j.error=cjson.decode(ARGV[4]); j.lease_until=0; j.token=''
  if j.error==cjson.null then j.status='done'
  elseif j.attempts>=j.max_attempts then j.status='dead'
  else j.status='queued' end
  j.due=now+tonumber(ARGV[5]); redis.call('HSET',KEYS[1],j.id,cjson.encode(j)); return 1
end
error('unknown operation')
"""


class RedisStore:
    """Dedicated namespace; no FLUSHDB. Enable Redis persistence for durability."""

    def __init__(
        self, url: str, namespace: str = "jobkit", *, concurrency_limit: int = 4
    ) -> None:
        if concurrency_limit < 1:
            raise ValueError("concurrency_limit must be positive")
        if not namespace or "{" in namespace or "}" in namespace:
            raise ValueError("namespace cannot be empty or contain braces")
        self.client = Redis.from_url(url, decode_responses=True)
        self.limit = concurrency_limit
        self.keys = [f"{{{namespace}}}:jobs", f"{{{namespace}}}:unique"]
        self.script = self.client.register_script(_SCRIPT)

    def enqueue(self, job: Job) -> str:
        validate_new(job)
        return str(self.script(keys=self.keys, args=["enqueue", encode(job)]))

    def claim(self, lease_seconds: float) -> Job | None:
        if lease_seconds <= 0 or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be positive")
        raw = self.script(
            keys=self.keys, args=["claim", lease_seconds, str(uuid.uuid4()), self.limit]
        )
        return decode(raw) if raw else None

    def finish(self, job: Job, error: str | None, retry_delay: float) -> bool:
        if retry_delay < 0 or not math.isfinite(retry_delay):
            raise ValueError("retry_delay must be nonnegative and finite")
        import json

        return bool(
            self.script(
                keys=self.keys,
                args=["finish", job.id, job.token, json.dumps(error), retry_delay],
            )
        )

    def get(self, job_id: str) -> Job | None:
        raw = self.client.hget(self.keys[0], job_id)
        if raw is None:
            return None
        if not isinstance(raw, (str, bytes)):
            raise TypeError("synchronous Redis client returned an invalid response")
        return decode(raw.decode() if isinstance(raw, bytes) else raw) if raw else None

    def dead_jobs(self) -> list[Job]:
        rows = self.client.hvals(self.keys[0])
        if not isinstance(rows, list):
            raise TypeError("synchronous Redis client returned an invalid response")
        jobs = [decode(raw.decode() if isinstance(raw, bytes) else raw) for raw in rows]
        return sorted(
            (j for j in jobs if j.status == "dead"), key=lambda j: (j.due, j.id)
        )

    def purge(self, job_id: str) -> bool:
        return bool(self.script(keys=self.keys, args=["purge", job_id]))
