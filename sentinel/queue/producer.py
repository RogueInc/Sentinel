"""
sentinel/queue/producer.py

Enqueues an incident for triage onto one of two priority streams.
Called after the Postgres INSERT commits (see ingest/routes.py) —
never before, so a worker can never pick up a row that isn't visible yet.
"""
import redis.asyncio as redis
from sentinel.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def enqueue(incident_id: str, priority: str = "warning") -> str:
    """priority: any Alertmanager severity label. 'critical' goes to the
    P1 stream and is drained first; everything else goes to P2."""
    from sentinel.obs.tracing import inject_context
    stream = settings.stream_p1 if priority == "critical" else settings.stream_p2
    r = get_redis()
    fields = inject_context({"incident_id": incident_id})
    msg_id = await r.xadd(stream, fields, maxlen=100_000, approximate=True)
    return msg_id


async def enqueue_resume(incident_id: str, decision: dict, priority: str = "critical") -> str:
    """Used by the /approve endpoint: re-enqueues an incident that's
    paused at interrupt(), carrying the human's decision."""
    import json
    from sentinel.obs.tracing import inject_context
    stream = settings.stream_p1 if priority == "critical" else settings.stream_p2
    r = get_redis()
    fields = inject_context({"incident_id": incident_id, "resume": json.dumps(decision)})
    return await r.xadd(stream, fields, maxlen=100_000, approximate=True)