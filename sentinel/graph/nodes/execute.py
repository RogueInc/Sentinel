"""
sentinel/graph/nodes/execute.py

Idempotent execution: an intent row (with a unique idempotency key) is
written before the side effect runs. If this node re-runs after a worker
crash, the INSERT ... ON CONFLICT DO NOTHING makes the second attempt a
no-op read instead of a second real action.
"""
import hashlib
import json

from sentinel.actions.executors import ACTIONS
from sentinel.db import tx, fetchrow
from sentinel.graph.state import TriageState


def stable_hash(params: dict) -> str:
    return hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:16]


async def execute(state: TriageState) -> dict:
    a = state["proposed_action"]
    key = f"{state['incident_id']}:{a['action']}:{stable_hash(a['params'])}"

    async with tx() as c:
        row = await c.fetchrow(
            """
            INSERT INTO actions (incident_id, action, params, tier, approved_by, status, idempotency_key)
            VALUES ($1,$2,$3,$4,$5,'executing',$6)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            state["incident_id"], a["action"], json.dumps(a["params"]), a["tier"],
            state.get("approval", {}).get("by", "policy"), key,
        )

    if row is None:
        prior = await fetchrow("SELECT status FROM actions WHERE idempotency_key = $1", key)
        return {"execution_result": {"status": "already_executed", "prior_status": prior["status"] if prior else None}}

    try:
        result = await ACTIONS[a["action"]](**a["params"])
        async with tx() as c:
            await c.execute(
                "UPDATE actions SET status = $1, dry_run_output = $2 WHERE id = $3",
                "succeeded" if result.get("ok") else "failed", json.dumps(result), row["id"],
            )
        return {"execution_result": result}
    except Exception as e:
        async with tx() as c:
            await c.execute("UPDATE actions SET status = 'failed' WHERE id = $1", row["id"])
        return {"execution_result": {"ok": False, "error": str(e)}, "errors": [f"execute failed: {e}"]}
