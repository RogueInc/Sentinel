"""sentinel/ingest/fingerprint.py"""
import hashlib


def fingerprint(alert: dict) -> str:
    # deliberately excludes instance/pod so a rolling restart doesn't create N incidents
    keys = ("alertname", "service", "severity")
    labels = alert.get("labels", {})
    return hashlib.sha256("|".join(labels.get(k, "") for k in keys).encode()).hexdigest()