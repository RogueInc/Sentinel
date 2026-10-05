"""
sentinel/queue/reclaim.py

Background loop: claims messages whose consumer died mid-processing
(pending longer than reclaim_idle_ms). Unlike the earlier version, this
one actually does something with what it reclaims: if the incident is
already terminal (resolved/dead/manually fixed), the message is acked
and discarded; otherwise it's handed to process_message for a real retry.

Run this as a periodic task inside the worker process (see worker/runner.py).
"""
import asyncio
import logging

from sentinel.config import settings
from sentinel.db import fetchrow
from sentinel.queue.consumer import CONSUMER_NAME, ack
from sentinel.queue.producer import get_redis

log = logging.getLogger("sentinel.reclaim")

TERMINAL_STATUSES = {"resolved", "dead"}


async def reclaim_once(stream: str) -> list[tuple[str, dict]]:
    """Returns [(msg_id, fields), ...] for messages that were reclaimed
    and still need real reprocessing. Messages belonging to an already-
    terminal incident are acked here directly and not returned."""
    r = get_redis()
    to_process: list[tuple[str, dict]] = []
    cursor = "0-0"

    while True:
        next_cursor, claimed, _deleted = await r.xautoclaim(
            stream, settings.consumer_group, CONSUMER_NAME,
            min_idle_time=settings.reclaim_idle_ms, start_id=cursor, count=50,
        )
        if not claimed:
            break

        for msg_id, fields in claimed:
            incident_id = fields.get("incident_id")
            row = await fetchrow("SELECT status FROM incidents WHERE id = $1", incident_id) if incident_id else None

            if row and row["status"] in TERMINAL_STATUSES:
                log.info("incident %s already %s, acking stale message %s without reprocessing",
                          incident_id, row["status"], msg_id)
                await ack(stream, msg_id)
            else:
                log.warning("reclaimed message %s for incident %s, will reprocess", msg_id, incident_id)
                to_process.append((msg_id, fields))

        cursor = next_cursor
        if next_cursor == "0-0":
            break

    return to_process


async def reclaim_loop(graph, process_message_fn, interval_seconds: int = 60):
    """Run forever alongside the main consume loop. `process_message_fn`
    is worker.runner.process_message — passed in rather than imported,
    to avoid a circular import between reclaim.py and runner.py."""
    while True:
        try:
            for stream in (settings.stream_p1, settings.stream_p2):
                to_process = await reclaim_once(stream)
                for msg_id, fields in to_process:
                    await process_message_fn(graph, stream, msg_id, fields)
        except Exception:
            log.exception("reclaim pass failed")
        await asyncio.sleep(interval_seconds)