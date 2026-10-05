"""
sentinel/graph/nodes/approval.py

The human gate. Tier 1 actions auto-execute; tier 2+ (or critical severity)
pause the graph via interrupt() until a human approves, rejects, or modifies.
"""
from langgraph.types import interrupt

from sentinel.actions.blast_radius import dry_run, estimate_blast_radius
from sentinel.actions.registry import ACTION_REGISTRY, validate_action
from sentinel.graph.state import TriageState


def needs_approval(state: TriageState) -> bool:
    a = state["proposed_action"]
    return ACTION_REGISTRY[a["action"]]["tier"] >= 2 or state["severity"] == "critical"


async def approval_gate(state: TriageState) -> dict:
    a = state["proposed_action"]
    if not needs_approval(state):
        return {"approval": {"decision": "auto", "by": "policy"}}

    dry = await dry_run(a)
    blast = await estimate_blast_radius(a)

    decision = interrupt({
        "incident": state["incident_id"],
        "top_hypothesis": state["hypotheses"][0] if state.get("hypotheses") else None,
        "evidence": [e["summary"] for e in state.get("evidence", [])],
        "proposed_action": a,
        "tier": ACTION_REGISTRY[a["action"]]["tier"],
        "reversible": ACTION_REGISTRY[a["action"]]["reversible"],
        "dry_run": dry,
        "blast_radius": blast,
    })

    if decision.get("decision") == "modify":
        validated = validate_action(decision["action"])
        return {"proposed_action": {**validated, "tier": ACTION_REGISTRY[validated["action"]]["tier"]},
                "approval": decision}
    return {"approval": decision}


def route_after_approval(state: TriageState) -> str:
    decision = state.get("approval", {}).get("decision")
    if decision == "reject":
        return "postmortem"
    return "execute"
