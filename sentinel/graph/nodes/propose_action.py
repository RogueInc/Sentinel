"""
sentinel/graph/nodes/propose_action.py

Asks the LLM to pick one action from the constrained registry and fill
its params, informed by the top hypothesis and any matching runbook.
The output is validated against ACTION_REGISTRY before it's trusted.
"""
import json

from sentinel.actions.registry import ACTION_REGISTRY, validate_action
from sentinel.graph.state import TriageState
from sentinel.graph.tools.runbook_search import matching_runbooks
from sentinel.llm.client import complete

PROPOSE_PROMPT = """Incident on service "{service}" (severity: {severity}).
Top hypothesis: {top_hypothesis}

Relevant runbook excerpts:
{runbooks}

Choose exactly one action from this registry that best addresses the top
hypothesis. Respond as JSON:
{{"action": "<one of: {actions}>", "params": {{...matching the action's required params...}}, "rationale": "<one sentence>"}}
Return ONLY the JSON object. If unsure, choose "no_action" or "page_secondary".
"""


async def propose_action(state: TriageState) -> dict:
    hyps = state.get("hypotheses", [])
    top = hyps[0] if hyps else {"cause": "unknown", "confidence": 0.0}

    try:
        runbooks = await matching_runbooks(top["cause"], k=2)
    except Exception:
        runbooks = []
    runbook_text = "\n".join(f"- {r['chunk'][:300]}" for r in runbooks) or "(none found)"

    prompt = PROPOSE_PROMPT.format(
        service=state["service"],
        severity=state["severity"],
        top_hypothesis=json.dumps(top),
        runbooks=runbook_text,
        actions=", ".join(ACTION_REGISTRY.keys()),
    )
    result = await complete(prompt, tenant_id=state.get("service", "default"), max_tokens=300)

    try:
        candidate = json.loads(result.text)
        validated = validate_action(candidate)
    except (json.JSONDecodeError, ValueError):
        validated = {"action": "page_secondary", "params": {"team": "on-call"}}

    return {
        "proposed_action": {
            **validated,
            "tier": ACTION_REGISTRY[validated["action"]]["tier"],
        },
        "tokens_used": result.tokens,
        "cost_usd": result.cost,
    }
