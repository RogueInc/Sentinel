"""sentinel/graph/state.py — the TriageState schema and its reducers."""
import operator
from typing import Annotated, TypedDict


class Evidence(TypedDict):
    kind: str          # metrics|logs|deploys|k8s|similar
    summary: str
    raw: dict
    weight: float


def merge_evidence(a: list[Evidence], b: list[Evidence]) -> list[Evidence]:
    """Reducer: parallel evidence-collector branches append and dedupe,
    never overwrite. Without this, only the last parallel branch to
    finish would survive in state."""
    seen = {(e["kind"], e["summary"]) for e in a}
    return a + [e for e in b if (e["kind"], e["summary"]) not in seen]


class Hypothesis(TypedDict):
    cause: str
    confidence: float
    supporting: list[str]
    contradicting: list[str]


class TriageState(TypedDict, total=False):
    incident_id: str
    service: str
    severity: str
    alert: dict

    is_duplicate: bool
    similar_incidents: list[dict]

    evidence: Annotated[list[Evidence], merge_evidence]
    evidence_gaps: Annotated[list[str], operator.add]
    hypotheses: list[Hypothesis]
    dig_count: int
    dig_query: dict | None

    proposed_action: dict | None
    approval: dict | None
    execution_result: dict | None
    verified: bool
    verify_count: int
    postmortem: str

    tokens_used: Annotated[int, operator.add]
    cost_usd: Annotated[float, operator.add]
    errors: Annotated[list[str], operator.add]
