"""
sentinel/graph/nodes/evidence.py

Fans out to four parallel evidence collectors via Send(), each bounded
by its own timeout so a slow Loki doesn't hang the whole triage.
"""
import asyncio
from datetime import datetime, timedelta, timezone

from langgraph.constants import END
from langgraph.types import Send

from sentinel.graph.state import TriageState
from sentinel.graph.tools import promql, loki, deploys, k8s

TOOL_TIMEOUT_SECONDS = 20


def fan_out_evidence(state: TriageState):
    if state.get("is_duplicate"):
        return END
    return [
        Send("collect", {"kind": k, "incident_id": state["incident_id"], "service": state["service"]})
        for k in ("metrics", "logs", "deploys", "k8s")
    ]


async def collect(state: dict) -> dict:
    """One invocation per evidence kind, run in parallel by fan_out_evidence."""
    kind = state["kind"]
    service = state["service"]

    try:
        summary, raw = await asyncio.wait_for(_collect_one(kind, service), timeout=TOOL_TIMEOUT_SECONDS)
        if summary is None:
            return {"evidence_gaps": [f"{kind} unavailable"]}
        return {"evidence": [{"kind": kind, "summary": summary, "raw": raw, "weight": 1.0}]}
    except asyncio.TimeoutError:
        return {"evidence_gaps": [f"{kind} timed out after {TOOL_TIMEOUT_SECONDS}s"]}
    except Exception as e:
        return {"evidence_gaps": [f"{kind} failed: {e}"]}


async def _collect_one(kind: str, service: str):
    if kind == "metrics":
        signals = await promql.golden_signal_summary(service)
        if not signals:
            return None, {}
        summary = "; ".join(f"{s['signal']}: {s['summary']}" for s in signals)
        return summary, {"signals": signals}

    if kind == "logs":
        top = await loki.top_errors(service, minutes=30)
        if not top:
            return None, {}
        summary = "; ".join(f"{t['count']}x '{t['template'][:60]}'" for t in top)
        return summary, {"top_errors": top}

    if kind == "deploys":
        since = datetime.now(timezone.utc) - timedelta(hours=1)
        changes = await deploys.recent_changes(service, since)
        if not changes:
            return None, {}
        return deploys.summarize(changes), {"changes": changes}

    if kind == "k8s":
        health = await k8s.pod_health(service)
        if not health or health.get("pod_count", 0) == 0:
            return None, {}
        return k8s.summarize(health), {"health": health}

    return None, {}
