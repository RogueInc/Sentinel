"""sentinel/graph/tools/promql.py"""
from datetime import datetime, timedelta, timezone
from statistics import mean
import httpx
from sentinel.config import settings

GOLDEN_SIGNALS = {
    "error_rate": 'sum(rate(http_requests_total{{service="{svc}",status=~"5.."}}[5m]))'
                  ' / sum(rate(http_requests_total{{service="{svc}"}}[5m]))',
    "latency_p95": 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{service="{svc}"}}[5m])) by (le))',
    "throughput": 'sum(rate(http_requests_total{{service="{svc}"}}[5m]))',
    "saturation": 'container_memory_working_set_bytes{{pod=~"{svc}.*"}}',
}


async def query_range(expr: str, minutes: int = 60, step: str = "60s") -> list[dict]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{settings.prometheus_url}/api/v1/query_range",
            params={"query": expr, "start": start.timestamp(), "end": end.timestamp(), "step": step},
        )
        r.raise_for_status()
        return r.json()["data"]["result"]


async def query_instant(expr: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{settings.prometheus_url}/api/v1/query", params={"query": expr})
        r.raise_for_status()
        return r.json()["data"]["result"]


def _is_step(series: list[float]) -> bool:
    if len(series) < 6:
        return False
    first_half = mean(series[:len(series) // 2])
    second_half = mean(series[len(series) // 2:])
    if first_half == 0:
        return second_half > 0.1
    return abs(second_half - first_half) / max(first_half, 1e-9) > 0.5


def _onset(series: list[float]) -> int:
    """Rough index (in steps) where the series visibly departs from baseline."""
    if len(series) < 4:
        return 0
    base = mean(series[:max(1, len(series) // 4)])
    threshold = base * 1.5 if base > 0 else 0.05
    for i, v in enumerate(series):
        if v > threshold:
            return len(series) - i
    return 0


def describe(series: list[float]) -> str:
    """Turn a raw numeric series into a short, deterministic sentence.
    Never feed raw time series to the LLM — describe the shape in code."""
    if not series:
        return "no data available"
    base = mean(series[:max(1, len(series) // 3)]) or 1e-9
    now = mean(series[-3:]) if len(series) >= 3 else series[-1]
    delta = (now - base) / base
    shape = "step change" if _is_step(series) else ("gradual ramp" if delta > 0.2 else "flat")
    return f"{shape}; baseline {base:.4g} -> current {now:.4g} ({delta:+.0%}), onset ~{_onset(series)} min ago"


async def golden_signal_summary(service: str) -> list[dict]:
    """Fetch and describe all four golden signals for a service. Used by
    the 'metrics' branch of gather_evidence."""
    out = []
    for name, tmpl in GOLDEN_SIGNALS.items():
        expr = tmpl.format(svc=service)
        try:
            result = await query_range(expr, minutes=60)
            if not result:
                continue
            values = [float(v) for _, v in result[0].get("values", [])]
            out.append({"signal": name, "summary": describe(values)})
        except Exception as e:
            out.append({"signal": name, "summary": f"query failed: {e}"})
    return out
