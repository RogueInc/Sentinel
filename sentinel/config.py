import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    postgres_dsn: str = os.getenv("POSTGRES_DSN", "postgresql://atlas:atlas@localhost:5432/atlas")
    prometheus_url: str = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
    loki_url: str = os.getenv("LOKI_URL", "http://localhost:3100")
    github_repo: str = os.getenv("GITHUB_REPO", "")
    github_token: str = os.getenv("GITHUB_TOKEN", "")
    llm_provider_url: str = os.getenv("LLM_PROVIDER_URL", "")
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_model: str = os.getenv("LLM_MODEL", "openai/gpt-oss-20b")

    # Phase 4 — queue / worker
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_p1: str = os.getenv("STREAM_P1", "triage:p1")
    stream_p2: str = os.getenv("STREAM_P2", "triage:p2")
    stream_dlq: str = os.getenv("STREAM_DLQ", "triage:dlq")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "workers")
    max_deliveries: int = int(os.getenv("MAX_DELIVERIES", "3"))
    reclaim_idle_ms: int = int(os.getenv("RECLAIM_IDLE_MS", "300000"))  # 5 min

    # Phase 5 — LLM reliability
    llm_fallback_provider_url: str = os.getenv("LLM_FALLBACK_PROVIDER_URL", "")
    llm_fallback_api_key: str = os.getenv("LLM_FALLBACK_API_KEY", "")
    llm_fallback_model: str = os.getenv("LLM_FALLBACK_MODEL", "")
    breaker_failure_threshold: int = int(os.getenv("BREAKER_FAILURE_THRESHOLD", "5"))
    breaker_cooldown_seconds: int = int(os.getenv("BREAKER_COOLDOWN_SECONDS", "30"))
    daily_budget_usd: float = float(os.getenv("DAILY_BUDGET_USD", "5.0"))
    per_incident_budget_usd: float = float(os.getenv("PER_INCIDENT_BUDGET_USD", "0.50"))
    semantic_cache_threshold: float = float(os.getenv("SEMANTIC_CACHE_THRESHOLD", "0.05"))
    ratelimit_rps: float = float(os.getenv("RATELIMIT_RPS", "5"))
    ratelimit_burst: int = int(os.getenv("RATELIMIT_BURST", "20"))

    # Phase 6 — observability
    otlp_endpoint: str = os.getenv("OTLP_ENDPOINT", "http://localhost:4317")
    service_name: str = os.getenv("OTEL_SERVICE_NAME", "sentinel")

settings = Settings()