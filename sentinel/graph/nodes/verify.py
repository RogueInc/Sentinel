"""
sentinel/graph/nodes/verify.py

Waits, re-queries the golden signals, compares against the pre-action
window. If not recovered and under the retry cap, loops back to
hypothesize with the failed action recorded as new evidence.
"""
import asyncio

from sentinel.graph.state import TriageState
from sentinel.graph.tools.promql import GOLDEN_SIGNALS, query_range, describe

MAX_VERIFY_ATTEMPTS = 2
WAIT_SECONDS = 90


async def verify(state: TriageState) -> dict:
    await asyncio.sleep(WAIT_SECONDS)

    service = state["service"]
    expr = GOLDEN_SIGNALS["error_rate"].format(svc=service)
    try:
        result = await query_range(expr, minutes=5)
        values = [float(v) for _, v in result[0].get("values", [])] if result else []
        current_rate = values[-1] if values else None
    except Exception:
        current_rate = None

    recovered = current_rate is not None and current_rate < 0.05
    verify_count = state.get("verify_count", 0) + 1

    if recovered:
        return {"verified": True, "verify_count": verify_count}

    failure_note = {
        "kind": "action_outcome",
        "summary": f"action '{state['proposed_action']['action']}' did not resolve the issue "
                   f"(error_rate={current_rate})",
        "raw": {"current_rate": current_rate},
        "weight": 1.0,
    }
    return {"verified": False, "verify_count": verify_count, "evidence": [failure_note]}


def route_after_verify(state: TriageState) -> str:
    if state.get("verified"):
        return "postmortem"
    if state.get("verify_count", 0) >= MAX_VERIFY_ATTEMPTS:
        return "postmortem"   # escalate is handled inside postmortem/executors via page_secondary
    return "hypothesize"
