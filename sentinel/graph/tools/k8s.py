"""
sentinel/graph/tools/k8s.py

Pod restarts, OOMKilled counts, pending pods, recent warning events.
Shells out to kubectl; returns a graceful empty result if no cluster
is configured yet so the graph keeps working without k8s in place.
"""
import asyncio
import json


async def _kubectl_json(*args: str) -> dict | list:
    try:
        proc = await asyncio.create_subprocess_exec(
            "kubectl", *args, "-o", "json",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return {}
        return json.loads(stdout)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


async def pod_health(service: str) -> dict:
    data = await _kubectl_json("get", "pods", "-l", f"app={service}")
    items = data.get("items", []) if isinstance(data, dict) else []

    restarts = 0
    oomkilled = 0
    pending = 0
    for pod in items:
        phase = pod.get("status", {}).get("phase")
        if phase == "Pending":
            pending += 1
        for cs in pod.get("status", {}).get("containerStatuses", []):
            restarts += cs.get("restartCount", 0)
            last_state = cs.get("lastState", {}).get("terminated", {})
            if last_state.get("reason") == "OOMKilled":
                oomkilled += 1

    return {
        "pod_count": len(items),
        "total_restarts": restarts,
        "oomkilled_count": oomkilled,
        "pending_count": pending,
    }


def summarize(health: dict) -> str:
    if not health or health.get("pod_count", 0) == 0:
        return "no k8s pod data available"
    parts = [f"{health['pod_count']} pods"]
    if health["total_restarts"]:
        parts.append(f"{health['total_restarts']} total restarts")
    if health["oomkilled_count"]:
        parts.append(f"{health['oomkilled_count']} OOMKilled")
    if health["pending_count"]:
        parts.append(f"{health['pending_count']} pending")
    return ", ".join(parts)
