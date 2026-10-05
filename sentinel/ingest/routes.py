"""
sentinel/ingest/routes.py

The Alertmanager webhook target. Rate-limited (an alert storm must not
knock over the thing that triages alert storms), then dedupes via the
partial unique index on incidents.fingerprint, enqueues newly-created
incidents only (an updated duplicate just bumps alert_count, no new
triage run).
"""
import json
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from sentinel.db import tx
from sentinel.ingest.fingerprint import fingerprint
from sentinel.ingest.suppress import suppressed
from sentinel.llm.ratelimit import allow
from sentinel.queue.producer import enqueue

router = APIRouter()


@router.post("/v1/alerts", status_code=202)
async def ingest(payload: dict):
    alerts = payload.get("alerts", [])

    # One rate-limit check per request, costed by batch size, so a
    # single Alertmanager payload carrying many alerts still counts
    # proportionally against the bucket rather than as one token.
    if not await allow("ingest", cost=max(1, len(alerts))):
        raise HTTPException(429, "rate limit exceeded")

    accepted = []
    for a in alerts:
        if a.get("status") == "resolved":
            fp = fingerprint(a)
            async with tx() as c:
                await c.execute(
                    "UPDATE incidents SET status = 'resolved', resolved_at = now() "
                    "WHERE fingerprint = $1 AND status <> 'resolved'", fp,
                )
            continue

        if await suppressed(a):
            continue

        fp = fingerprint(a)
        labels = a.get("labels", {})
        annotations = a.get("annotations", {})

        async with tx() as c:
            row = await c.fetchrow(
                """
                INSERT INTO incidents (id, fingerprint, service, severity, status, title)
                VALUES ($1,$2,$3,$4,'open',$5)
                ON CONFLICT (fingerprint) WHERE status <> 'resolved'
                DO UPDATE SET alert_count = incidents.alert_count + 1, last_seen = now()
                RETURNING id, (xmax = 0) AS created
                """,
                uuid4(), fp, labels.get("service", "unknown"), labels.get("severity", "warning"),
                annotations.get("summary", "(no summary)"),
            )
            await c.execute(
                "INSERT INTO alerts (incident_id, raw) VALUES ($1,$2)", row["id"], json.dumps(a)
            )

        if row["created"]:
            await enqueue(str(row["id"]), priority=labels.get("severity", "warning"))
            accepted.append(str(row["id"]))

    return {"accepted": accepted}