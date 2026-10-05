"""sentinel/actions/registry.py — the constrained action space. propose_action
can only emit keys defined here; nothing outside this registry is executable."""

ACTION_REGISTRY = {
    "restart_pod":     {"tier": 2, "reversible": True,  "params": ["namespace", "pod"]},
    "rollback_deploy": {"tier": 2, "reversible": True,  "params": ["deployment", "to_revision"]},
    "scale_replicas":  {"tier": 2, "reversible": True,  "params": ["deployment", "replicas"]},
    "clear_cache":     {"tier": 2, "reversible": False, "params": ["service"]},
    "drain_node":      {"tier": 3, "reversible": False, "params": ["node"]},
    "page_secondary":  {"tier": 1, "reversible": True,  "params": ["team"]},
    "no_action":       {"tier": 1, "reversible": True,  "params": []},
}


def validate_action(candidate: dict) -> dict:
    """Raises ValueError if the action or its params don't match the
    registry. Call this on every LLM-proposed action before it's stored
    or executed."""
    action = candidate.get("action")
    if action not in ACTION_REGISTRY:
        raise ValueError(f"unknown action '{action}' — not in ACTION_REGISTRY")

    spec = ACTION_REGISTRY[action]
    params = candidate.get("params", {})
    missing = [p for p in spec["params"] if p not in params]
    if missing:
        raise ValueError(f"action '{action}' missing required params: {missing}")

    return {"action": action, "params": {k: params[k] for k in spec["params"]}}
