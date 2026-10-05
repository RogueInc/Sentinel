"""
sentinel/api/sse.py

Polls incident_events (written by worker/runner.py's emit_event) rather
than holding an in-process queue, so any API replica can serve the
stream for an incident running on any worker.
"""
import asyncio
import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from sentinel.db import fetch, fetchrow

router = APIRouter()

POLL_INTERVAL_SECONDS = 0.5
TERMINAL_STATUSES = {"resolved", "dead", "escalated"}


@router.get("/v1/incidents/{incident_id}/stream")
async def stream(incident_id: str, last_seq: int = 0):
    async def gen():
        seq = last_seq
        while True:
            rows = await fetch(
                "SELECT seq, kind, data, created_at FROM incident_events "
                "WHERE incident_id = $1 AND seq > $2 ORDER BY seq", incident_id, seq,
            )
            for row in rows:
                seq = row["seq"]
                payload = json.dumps({"kind": row["kind"], "data": row["data"]}, default=str)
                # "error" is renamed to "node_error" on the wire only —
                # EventSource's addEventListener('error', ...) collides
                # with the browser's own built-in connection-error event
                # of the same name, which fires on every auto-reconnect
                # and would otherwise be indistinguishable from a real
                # node failure.
                event_name = "node_error" if row["kind"] == "error" else row["kind"]
                yield f"id: {row['seq']}\nevent: {event_name}\ndata: {payload}\n\n"

            status_row = await fetchrow("SELECT status FROM incidents WHERE id = $1", incident_id)
            if status_row and status_row["status"] in TERMINAL_STATUSES:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    return StreamingResponse(gen(), media_type="text/event-stream")