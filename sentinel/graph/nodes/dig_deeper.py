"""
sentinel/graph/nodes/dig_deeper.py

Conditional loop when the top hypothesis has low confidence. The model
names one specific missing query; we run it and merge the result back
into evidence. Bounded by dig_count so this can never loop forever.
"""
import json

from sentinel.graph.state import TriageState
from sentinel.graph.tools import promql
from sentinel.llm.client import complete

MAX_DIGS = 2
CONFIDENCE_FLOOR = 0.6

DIG_PROMPT = """The current top hypothesis for this incident has confidence {confidence:.2f}, below the {floor} threshold.
Hypotheses so far: {hypotheses}

Name ONE additional PromQL query (against Prometheus) that would most help
confirm or refute the top hypothesis. Respond as JSON:
{{"query": "<promql expression>", "reason": "<why this helps>"}}
Return ONLY the JSON object.
"""


def needs_dig(state: TriageState) -> str:
    hyps = state.get("hypotheses", [])
    top_conf = hyps[0]["confidence"] if hyps else 0.0
    if top_conf >= CONFIDENCE_FLOOR or state.get("dig_count", 0) >= MAX_DIGS:
        return "propose_action"
    return "dig_deeper"


async def dig_deeper(state: TriageState) -> dict:
    prompt = DIG_PROMPT.format(
        confidence=state["hypotheses"][0]["confidence"] if state.get("hypotheses") else 0.0,
        floor=CONFIDENCE_FLOOR,
        hypotheses=json.dumps(state.get("hypotheses", [])),
    )
    result = await complete(prompt, tenant_id=state.get("service", "default"), max_tokens=200)

    try:
        query = json.loads(result.text)
    except json.JSONDecodeError:
        query = {"query": f'up{{service="{state["service"]}"}}', "reason": "fallback: model output unparsable"}

    new_evidence = []
    try:
        series = await promql.query_instant(query["query"])
        summary = f"dig query '{query['query']}' -> {len(series)} series returned"
        new_evidence.append({"kind": "metrics", "summary": summary, "raw": {"series": series}, "weight": 0.5})
    except Exception as e:
        new_evidence.append({"kind": "metrics", "summary": f"dig query failed: {e}", "raw": {}, "weight": 0.5})

    return {
        "evidence": new_evidence,
        "dig_count": state.get("dig_count", 0) + 1,
        "tokens_used": result.tokens,
        "cost_usd": result.cost,
    }
