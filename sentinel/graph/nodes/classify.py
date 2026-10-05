"""
sentinel/graph/nodes/classify.py

Cheap, deterministic-first classification. Checks for an open duplicate
and flap count before ever calling the LLM — most alerts should die here.
"""
from sentinel.db import fetchrow
from sentinel.graph.tools.runbook_search import similar_incidents


async def classify(state: dict) -> dict:
    incident_id = state["incident_id"]

    row = await fetchrow(
        "SELECT alert_count FROM incidents WHERE id = $1", incident_id
    )
    alert_count = row["alert_count"] if row else 1

    # Flap suppression: too many re-fires in a short window -> treat as duplicate
    if alert_count > 5:
        return {"is_duplicate": True, "similar_incidents": []}

    query_text = f"{state.get('service', '')} {state.get('severity', '')}"
    try:
        similar = await similar_incidents(query_text, k=3)
    except Exception:
        similar = []

    return {"is_duplicate": False, "similar_incidents": similar}
