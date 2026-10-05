"""
sentinel/llm/budget.py

Per-incident and per-day spend caps, tracked in Redis. Raising
BudgetExceeded lets the graph route to a second interrupt() asking a
human to authorise more, rather than silently overspending.
"""
from datetime import date

from sentinel.config import settings
from sentinel.queue.producer import get_redis  # reuses the same Redis client


class BudgetExceeded(Exception):
    def __init__(self, scope: str, spent: float, cap: float):
        self.scope, self.spent, self.cap = scope, spent, cap
        super().__init__(f"{scope} budget exceeded: ${spent:.4f} spent of ${cap:.2f} cap")


async def assert_within(incident_id: str, tenant_id: str = "default"):
    r = get_redis()

    incident_key = f"budget:incident:{incident_id}"
    incident_spent = float(await r.get(incident_key) or 0.0)
    if incident_spent >= settings.per_incident_budget_usd:
        raise BudgetExceeded("incident", incident_spent, settings.per_incident_budget_usd)

    day_key = f"budget:day:{tenant_id}:{date.today().isoformat()}"
    day_spent = float(await r.get(day_key) or 0.0)
    if day_spent >= settings.daily_budget_usd:
        raise BudgetExceeded("daily", day_spent, settings.daily_budget_usd)


async def charge(incident_id: str, cost: float, tenant_id: str = "default"):
    if cost <= 0:
        return
    r = get_redis()

    incident_key = f"budget:incident:{incident_id}"
    await r.incrbyfloat(incident_key, cost)
    await r.expire(incident_key, 86400)  # 1 day TTL, incidents don't run longer than that

    day_key = f"budget:day:{tenant_id}:{date.today().isoformat()}"
    await r.incrbyfloat(day_key, cost)
    await r.expire(day_key, 172800)  # 2 days, comfortably past midnight rollover