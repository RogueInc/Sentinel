"""
sentinel/actions/blast_radius.py

Estimates the consequence of a proposed action (pods affected, traffic %)
and produces a dry-run description — both shown to the human at the
approval gate so they're not approving blind.
"""
import asyncio
import json


async def estimate_blast_radius(action: dict) -> dict:
    a = action["action"]
    params = action.get("params", {})

    if a == "restart_pod":
        return {"pods_affected": 1, "traffic_pct": "~0% (rolling)", "note": "single pod restart"}
    if a == "rollback_deploy":
        return {"pods_affected": "all replicas of " + params.get("deployment", "?"),
                 "traffic_pct": "100% over rollout duration", "note": "reverts to a prior revision"}
    if a == "scale_replicas":
        return {"pods_affected": f"scaling to {params.get('replicas', '?')} replicas",
                 "traffic_pct": "0% (additive)", "note": "no existing pods terminated"}
    if a == "clear_cache":
        return {"pods_affected": 0, "traffic_pct": "possible latency spike on cache miss",
                 "note": "irreversible — cache must rebuild"}
    if a == "drain_node":
        return {"pods_affected": "all pods on node " + params.get("node", "?"),
                 "traffic_pct": "up to 100% of that node's share", "note": "destructive, tier 3"}
    if a == "page_secondary":
        return {"pods_affected": 0, "traffic_pct": "0%", "note": "notification only, no infra change"}
    return {"pods_affected": 0, "traffic_pct": "0%", "note": "no-op"}


async def dry_run(action: dict) -> dict:
    """kubectl --dry-run=server for the applicable actions; a structural
    diff otherwise. Returns a serializable summary for the approval card."""
    a = action["action"]
    params = action.get("params", {})

    if a == "rollback_deploy":
        cmd = ["kubectl", "rollout", "undo", f"deployment/{params.get('deployment')}",
               f"--to-revision={params.get('to_revision')}", "--dry-run=server"]
    elif a == "scale_replicas":
        cmd = ["kubectl", "scale", f"deployment/{params.get('deployment')}",
               f"--replicas={params.get('replicas')}", "--dry-run=server"]
    elif a == "restart_pod":
        cmd = ["kubectl", "delete", "pod", params.get("pod", ""), "-n", params.get("namespace", "default"),
               "--dry-run=server"]
    else:
        return {"dry_run": "n/a for this action type", "action": a, "params": params}

    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        return {"command": " ".join(cmd), "output": (stdout or stderr).decode()[:500]}
    except FileNotFoundError:
        return {"command": " ".join(cmd), "output": "kubectl not available — dry-run skipped"}
