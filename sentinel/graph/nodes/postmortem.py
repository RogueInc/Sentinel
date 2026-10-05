"""
sentinel/graph/nodes/postmortem.py

Builds a factual timeline in code, asks the LLM only for the narrative,
then writes the incident back as resolved with its embedding so future
incidents can find it via runbook_search.similar_incidents().
"""
import json

from sentinel.db import fetch, tx
from sentinel.graph.state import TriageState
from sentinel.graph.tools.runbook_search import embed
from sentinel.llm.client import complete

POSTMORTEM_PROMPT = """Write a concise postmortem narrative (4-6 sentences) for this incident.
Service: {service}, severity: {severity}
Timeline: {timeline}
Root cause: {cause}
Action taken: {action}
Outcome: {outcome}

Be factual and specific. No speculation beyond what's given.
"""


async def _build_timeline(incident_id: str) -> list[dict]:
    alerts = await fetch("SELECT received_at, raw FROM alerts WHERE incident_id = $1 ORDER BY received_at", incident_id)
    actions = await fetch("SELECT created_at, action, status FROM actions WHERE incident_id = $1 ORDER BY created_at", incident_id)
    timeline = [{"at": str(a["received_at"]), "event": "alert received"} for a in alerts]
    timeline += [{"at": str(a["created_at"]), "event": f"action {a['action']} -> {a['status']}"} for a in actions]
    timeline.sort(key=lambda e: e["at"])
    return timeline


async def postmortem(state: TriageState) -> dict:
    incident_id = state["incident_id"]
    timeline = await _build_timeline(incident_id)
    top = state["hypotheses"][0] if state.get("hypotheses") else {"cause": "undetermined"}
    action = state.get("proposed_action", {}).get("action", "none")
    outcome = "resolved" if state.get("verified") else "escalated" if state.get("proposed_action") else "no action taken"

    prompt = POSTMORTEM_PROMPT.format(
        service=state["service"], severity=state["severity"],
        timeline=json.dumps(timeline), cause=top["cause"], action=action, outcome=outcome,
    )
    result = await complete(prompt, tenant_id=state.get("service", "default"), max_tokens=400, json_mode=False)

    title = f"{state['service']}: {top['cause']}"[:200]
    vec = embed(f"{title} {result.text}")

    async with tx() as c:
        await c.execute(
            """
            UPDATE incidents
            SET status = 'resolved', resolved_at = now(), hypothesis = $1,
                embedding = $2, title = $3,
                mttr_seconds = EXTRACT(EPOCH FROM (now() - first_seen))::int
            WHERE id = $4
            """,
            json.dumps(top), vec, title, incident_id,
        )

    return {"postmortem": result.text, "tokens_used": result.tokens, "cost_usd": result.cost}
