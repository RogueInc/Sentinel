"""
sentinel/api/routes_incidents.py

Approve/reject/modify an incident paused at approval_gate's interrupt(),
and a simple list endpoint for the UI.
"""
from fastapi import APIRouter, HTTPException

from sentinel.db import fetch, fetchrow
from sentinel.queue.producer import enqueue_resume

router = APIRouter()


@router.get("/v1/incidents")
async def list_incidents(status: str | None = None):
    if status:
        rows = await fetch(
            "SELECT id, service, severity, status, title, first_seen FROM incidents "
            "WHERE status = $1 ORDER BY first_seen DESC LIMIT 50", status,
        )
    else:
        rows = await fetch(
            "SELECT id, service, severity, status, title, first_seen FROM incidents "
            "ORDER BY first_seen DESC LIMIT 50",
        )
    return [dict(r) for r in rows]


@router.get("/v1/incidents/{incident_id}")
async def get_incident(incident_id: str):
    row = await fetchrow(
        """
        SELECT id, fingerprint, service, severity, status, title, alert_count,
               first_seen, last_seen, resolved_at, hypothesis, mttr_seconds
        FROM incidents WHERE id = $1
        """,
        incident_id,
    )
    if not row:
        raise HTTPException(404, "incident not found")
    return dict(row)


@router.post("/v1/incidents/{incident_id}/approve")
async def approve(incident_id: str, body: dict):
    """body: {"decision": "approve"|"reject"|"modify", "action": {...} (if modify)}"""
    row = await fetchrow("SELECT severity, status FROM incidents WHERE id = $1", incident_id)
    if not row:
        raise HTTPException(404, "incident not found")
    if row["status"] != "awaiting_approval":
        raise HTTPException(409, f"incident is not awaiting approval (status={row['status']})")

    decision = {
        "decision": {"approve": "auto", "reject": "reject", "modify": "modify"}.get(body["decision"], "reject"),
        "action": body.get("action"),
        "by": body.get("by", "on-call"),
    }
    await enqueue_resume(incident_id, decision, priority=row["severity"])
    return {"ok": True}