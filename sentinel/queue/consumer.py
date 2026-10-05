"""
sentinel/queue/consumer.py

Consumer-group reader. P1 is drained first, then P2 — a 4-minute P2
triage must never delay a P1 incident. Ensures the consumer group
exists (idempotent — BUSYGROUP is expected and ignored on every
restart after the first).
"""
import socket
from redis.exceptions import ResponseError
from sentinel.config import settings
from sentinel.queue.producer import get_redis

CONSUMER_NAME = socket.gethostname()


async def ensure_group(stream: str):
    r = get_redis()
    try:
        await r.xgroup_create(stream, settings.consumer_group, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def consume_one(block_ms: int = 2000):
    """Reads one message, preferring P1. Returns (stream, msg_id, fields)
    or None if nothing was available in this poll."""
    r = get_redis()
    for stream in (settings.stream_p1, settings.stream_p2):
        # No `block` argument at all -> non-blocking peek: XREADGROUP
        # returns immediately whether or not a message is waiting.
        # (block=0 means "block forever" in Redis, not "don't block" —
        # passing it here was the bug: the client's own socket timeout
        # fired first, surfacing as a spurious redis.exceptions.TimeoutError.)
        resp = await r.xreadgroup(
            settings.consumer_group, CONSUMER_NAME, {stream: ">"}, count=1
        )
        if resp:
            _, entries = resp[0]
            msg_id, fields = entries[0]
            return stream, msg_id, fields

    # nothing pending on either stream right now — do one short blocking
    # read on P1 so the worker isn't busy-polling
    resp = await r.xreadgroup(
        settings.consumer_group, CONSUMER_NAME, {settings.stream_p1: ">"}, count=1, block=block_ms
    )
    if resp:
        _, entries = resp[0]
        msg_id, fields = entries[0]
        return settings.stream_p1, msg_id, fields
    return None


async def ack(stream: str, msg_id: str):
    r = get_redis()
    await r.xack(stream, settings.consumer_group, msg_id)