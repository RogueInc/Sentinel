"""sentinel/obs/metrics.py"""
from prometheus_client import Counter, Gauge, Histogram

TRIAGE_DURATION = Histogram("sentinel_triage_seconds", "", ["severity", "outcome"])
TIME_TO_HYPOTHESIS = Histogram("sentinel_time_to_hypothesis_seconds", "")
HYPOTHESIS_ACCEPT = Counter("sentinel_hypothesis_total", "", ["verdict"])
ACTION_OUTCOME = Counter("sentinel_action_total", "", ["action", "result"])
DEDUPE_RATIO = Gauge("sentinel_dedupe_ratio", "")
QUEUE_DEPTH = Gauge("sentinel_queue_depth", "", ["lane"])
TOKENS = Counter("sentinel_tokens_total", "", ["node"])