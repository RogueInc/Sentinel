"""
sentinel/graph/tools/loki.py

Fetches logs for the incident window and clusters them into templates
before summarising, so the LLM interprets counts + examples, not raw lines.
"""
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
import httpx
from sentinel.config import settings

_DIGIT_RE = re.compile(r"\d+")
_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def _template(line: str) -> str:
    """Collapse a log line into a reusable template key by stripping
    volatile tokens (ids, numbers)."""
    line = _UUID_RE.sub("<uuid>", line)
    line = _DIGIT_RE.sub("<n>", line)
    return line[:160]


async def fetch_logs(service: str, minutes: int = 30) -> list[str]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    query = f'{{service="{service}"}} |= "error"'
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{settings.loki_url}/loki/api/v1/query_range",
            params={
                "query": query,
                "start": int(start.timestamp() * 1e9),
                "end": int(end.timestamp() * 1e9),
                "limit": 500,
            },
        )
        r.raise_for_status()
        streams = r.json().get("data", {}).get("result", [])
    lines = []
    for s in streams:
        for _, line in s.get("values", []):
            lines.append(line)
    return lines


def cluster_top(lines: list[str], top_n: int = 5) -> list[dict]:
    counts = Counter(_template(l) for l in lines)
    templates = {}
    for line in lines:
        t = _template(line)
        templates.setdefault(t, line)
    return [
        {"template": t, "count": c, "example": templates[t]}
        for t, c in counts.most_common(top_n)
    ]


async def top_errors(service: str, minutes: int = 30) -> list[dict]:
    """Public entrypoint used by the 'logs' branch of gather_evidence."""
    try:
        lines = await fetch_logs(service, minutes)
    except Exception:
        return []
    if not lines:
        return []
    return cluster_top(lines)
