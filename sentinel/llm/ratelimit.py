"""
sentinel/llm/ratelimit.py

Token-bucket rate limiter as a Redis Lua script so refill + consume
are atomic under concurrent callers. Applied to the ingest endpoint —
an alert storm must not knock over the thing that triages alert storms.
"""
import time

from sentinel.config import settings
from sentinel.queue.producer import get_redis

_LUA = """
local key, rate, burst, now, cost = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local b = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(b[1]) or burst
local ts = tonumber(b[2]) or now
tokens = math.min(burst, tokens + (now - ts) * rate)
if tokens < cost then
  return 0
end
redis.call('HMSET', key, 'tokens', tokens - cost, 'ts', now)
redis.call('EXPIRE', key, 3600)
return 1
"""

_script = None


async def allow(key: str, cost: float = 1.0) -> bool:
    global _script
    r = get_redis()
    if _script is None:
        _script = r.register_script(_LUA)
    result = await _script(
        keys=[f"ratelimit:{key}"],
        args=[settings.ratelimit_rps, settings.ratelimit_burst, time.time(), cost],
    )
    return bool(result)