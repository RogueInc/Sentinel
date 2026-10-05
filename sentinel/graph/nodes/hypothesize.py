"""
sentinel/graph/nodes/hypothesize.py

Asks the LLM for 3 ranked root-cause candidates, each required to cite
evidence by id. Citations are verified in code afterward — any hypothesis
that cites an evidence id that doesn't exist is dropped, not trusted.
"""
import json

from sentinel.graph.state import TriageState
from sentinel.llm.client import complete

HYPOTHESIZE_PROMPT = """You are an SRE. Below is evidence collected for an incident on service "{service}" (severity: {severity}).
Produce exactly 3 candidate root causes as a JSON array:
[{{"cause": str, "confidence": 0.0-1.0, "supporting": [evidence ids], "contradicting": [evidence ids]}}]

Rules:
- Every cause must cite at least one evidence id from the list below. No citation => do not include it.
- If evidence is insufficient, return confidence < 0.4 and name what is missing in "cause".
- Prefer temporal correlation (what changed just before onset) over plausibility.
- Return ONLY the JSON array, no other text.

EVIDENCE:
{evidence}
"""


def _format_evidence(evidence: list[dict]) -> str:
    return "\n".join(f"[{i}] ({e['kind']}) {e['summary']}" for i, e in enumerate(evidence))


def _validate(hypotheses: list[dict], evidence_count: int) -> list[dict]:
    """Drop any hypothesis whose citations reference an evidence index
    that doesn't actually exist -- the guardrail against fabricated support."""
    valid = []
    for h in hypotheses:
        supporting = [i for i in h.get("supporting", []) if isinstance(i, int) and 0 <= i < evidence_count]
        if not supporting:
            continue
        h["supporting"] = supporting
        valid.append(h)
    return valid


async def hypothesize(state: TriageState) -> dict:
    evidence = state.get("evidence", [])
    prompt = HYPOTHESIZE_PROMPT.format(
        service=state["service"],
        severity=state["severity"],
        evidence=_format_evidence(evidence) or "(no evidence collected)",
    )
    result = await complete(prompt, tenant_id=state.get("service", "default"), max_tokens=600)

    try:
        raw = json.loads(result.text)
    except json.JSONDecodeError:
        raw = []

    hypotheses = _validate(raw, len(evidence))
    hypotheses.sort(key=lambda h: h.get("confidence", 0), reverse=True)

    return {
        "hypotheses": hypotheses,
        "tokens_used": result.tokens,
        "cost_usd": result.cost,
    }
