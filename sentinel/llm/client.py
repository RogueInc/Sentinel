"""
sentinel/llm/client.py

Full reliability-wrapped LLM client: budget check -> cache lookup ->
circuit breaker -> retry -> provider, with a fallback provider on
primary failure. This is the file every node in graph/nodes/ calls;
upgrading it upgrades reliability everywhere at once.

pip install httpx tenacity
"""
import json
import logging

import httpx
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from sentinel.config import settings
from sentinel.llm.breaker import get_breaker
from sentinel.llm.budget import assert_within, charge, BudgetExceeded
from sentinel.llm.cache import get as cache_get, put as cache_put

log = logging.getLogger("sentinel.llm")


@dataclass
class LLMResult:
    text: str
    tokens: int = 0
    cost: float = 0.0


class AllProvidersDown(Exception):
    pass


_PROVIDERS = [
    (settings.llm_provider_url, settings.llm_api_key, settings.llm_model, "primary"),
    (settings.llm_fallback_provider_url, settings.llm_fallback_api_key, settings.llm_fallback_model, "fallback"),
]


@retry(
    wait=wait_exponential_jitter(initial=1, max=20),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,
)
async def _call_provider(url: str, api_key: str, model: str, prompt: str,
                          temperature: float, max_tokens: int, json_mode: bool) -> LLMResult:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "max_tokens": max_tokens,
                **({"response_format": {"type": "json_object"}} if json_mode else {}),
            },
        )
        if resp.status_code == 429 or resp.status_code >= 500:
            resp.raise_for_status()  # triggers tenacity retry
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        tokens = usage.get("total_tokens", 0)
        cost = tokens * 0.0000005  # placeholder rate; adjust per provider
        return LLMResult(text=text, tokens=tokens, cost=cost)


def _fallback_response(prompt: str) -> str:
    """No provider configured at all — return a structurally valid stub
    so downstream JSON parsing doesn't explode while wiring things up.

    Matches on phrases unique to each prompt template, NOT on generic
    words like "hypothesis" — that word appears in all three prompts
    (hypothesize, dig_deeper, propose_action), so a loose match there
    caused dig_deeper and propose_action to wrongly receive the
    hypotheses-list shape instead of their own dict shape.
    """
    p = prompt.lower()
    if "additional promql query" in p:
        return json.dumps({"query": "up", "reason": "no LLM provider configured"})
    if "choose exactly one action from this registry" in p:
        return json.dumps({"action": "no_action", "params": {}, "rationale": "no LLM provider configured"})
    if "candidate root causes" in p:
        return json.dumps([
            {"cause": "insufficient evidence to determine root cause",
             "confidence": 0.2, "supporting": [], "contradicting": []}
        ])
    # unrecognized prompt shape — safest default is a no-op action
    return json.dumps({"action": "no_action", "params": {}, "rationale": "no LLM provider configured"})


async def complete(prompt: str, tenant_id: str = "default", incident_id: str | None = None,
                    max_tokens: int = 800, temperature: float = 0.0, json_mode: bool = True) -> LLMResult:
    if not settings.llm_provider_url:
        return LLMResult(text=_fallback_response(prompt), tokens=0, cost=0.0)

    if incident_id:
        await assert_within(incident_id, tenant_id)  # raises BudgetExceeded

    model = settings.llm_model
    if temperature == 0.0:
        cached = await cache_get(prompt, model)
        if cached:
            return LLMResult(**cached)

    last_error = None
    for url, api_key, model_name, name in _PROVIDERS:
        if not url:
            continue
        breaker = get_breaker(name)
        if not breaker.allow():
            log.warning("breaker open for %s, skipping", name)
            continue
        try:
            result = await _call_provider(url, api_key, model_name, prompt, temperature, max_tokens, json_mode)
            breaker.record(True)
            if temperature == 0.0:
                await cache_put(prompt, model, {"text": result.text, "tokens": result.tokens, "cost": result.cost})
            if incident_id:
                await charge(incident_id, result.cost, tenant_id)
            return result
        except Exception as e:
            breaker.record(False)
            last_error = e
            log.warning("provider %s failed: %s", name, e)

    raise AllProvidersDown(f"all providers exhausted, last error: {last_error}")