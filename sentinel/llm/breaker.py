"""
sentinel/llm/breaker.py

Simple circuit breaker: opens after N consecutive failures, half-opens
after a cooldown to probe recovery. One instance per provider name.
"""
import time

from sentinel.config import settings


class CircuitBreaker:
    def __init__(self, threshold: int | None = None, cooldown: int | None = None):
        self.threshold = threshold or settings.breaker_failure_threshold
        self.cooldown = cooldown or settings.breaker_cooldown_seconds
        self.failures = 0
        self.opened_at: float | None = None

    def allow(self) -> bool:
        if self.opened_at is None:
            return True
        if time.monotonic() - self.opened_at > self.cooldown:
            # half-open: let one probe through
            return True
        return False

    def record(self, ok: bool):
        if ok:
            self.failures = 0
            self.opened_at = None
        else:
            self.failures += 1
            if self.failures >= self.threshold:
                self.opened_at = time.monotonic()

    @property
    def is_open(self) -> bool:
        return self.opened_at is not None and (time.monotonic() - self.opened_at) <= self.cooldown


_breakers: dict[str, CircuitBreaker] = {}


def get_breaker(provider: str) -> CircuitBreaker:
    if provider not in _breakers:
        _breakers[provider] = CircuitBreaker()
    return _breakers[provider]