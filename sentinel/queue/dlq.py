"""
sentinel/queue/dlq.py

Delivery-count tracking and dead-lettering. Redis Streams doesn't track
per-message delivery counts natively the way some brokers do, so we
track it ourselves via XPENDING's delivery-count field (returned on
detailed XPENDING queries) each time a message is reclaimed or fails.
"""
import json
import logging

from sentinel.config import settings
from sentinel.db import execute
from sentinel.queue.consumer import ack
from sentinel.queue.producer import get_redis

log = logging.getLogger("sentinel.dlq")


async def delivery_count(stream: str, msg_id: str) -> int:
    r = get_redis()
    detail = await r.xpending_range(stream, settings.consumer_group, min=msg_id, max=msg_id, count=1)
    if not detail:
        return 1
    return detail[0]["times_delivered"]


async def handle_failure(stream: str, msg_id: str, fields: dict, error: str):
    """Call this when processing a message raises. Either the message
    stays pending for reclaim, or — past max_deliveries — it's moved
    to the DLQ, the incident is marked dead, and a human is paged."""
    count = await delivery_count(stream, msg_id)
    incident_id = fields.get("incident_id")

    if count < settings.max_deliveries:
        log.warning("incident %s failed (attempt %d/%d): %s", incident_id, count, settings.max_deliveries, error)
        return  # leave pending; reclaim_loop or another consumer will retry

    log.error("incident %s exceeded max deliveries, moving to DLQ: %s", incident_id, error)
    r = get_redis()
    await r.xadd(settings.stream_dlq, {**fields, "reason": error, "delivery_count": str(count)})
    await ack(stream, msg_id)

    if incident_id:
        await execute("UPDATE incidents SET status = 'dead' WHERE id = $1", incident_id)
        await _page_human(incident_id, error)


async def _page_human(incident_id: str, error: str):
    """A triage system that silently drops incidents is a liability —
    wire this to your actual paging webhook (PagerDuty/Opsgenie/Slack)."""
    log.critical("PAGE: incident %s moved to DLQ and needs human attention: %s", incident_id, error)