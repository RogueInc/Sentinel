"""
sentinel/actions/executors.py

The dispatch table `ACTIONS` used by graph/nodes/execute.py. Each function
performs the actual side effect for one registry action. Kept separate from
the registry so the registry (contract) and executors (implementation) can
evolve independently.
"""
import asyncio


async def restart_pod(namespace: str, pod: str) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "delete", "pod", pod, "-n", namespace,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {"ok": proc.returncode == 0, "output": (stdout or stderr).decode()}


async def rollback_deploy(deployment: str, to_revision: int) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "rollout", "undo", f"deployment/{deployment}", f"--to-revision={to_revision}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {"ok": proc.returncode == 0, "output": (stdout or stderr).decode()}


async def scale_replicas(deployment: str, replicas: int) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "scale", f"deployment/{deployment}", f"--replicas={replicas}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {"ok": proc.returncode == 0, "output": (stdout or stderr).decode()}


async def clear_cache(service: str) -> dict:
    # placeholder — wire to your actual cache-clear endpoint/command
    return {"ok": True, "output": f"cache-clear signal sent to {service} (stub)"}


async def drain_node(node: str) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "drain", node, "--ignore-daemonsets", "--delete-emptydir-data",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {"ok": proc.returncode == 0, "output": (stdout or stderr).decode()}


async def page_secondary(team: str) -> dict:
    # placeholder — wire to PagerDuty/Opsgenie/Slack webhook
    return {"ok": True, "output": f"paged {team} (stub)"}


async def no_action() -> dict:
    return {"ok": True, "output": "no action taken"}


ACTIONS = {
    "restart_pod": restart_pod,
    "rollback_deploy": rollback_deploy,
    "scale_replicas": scale_replicas,
    "clear_cache": clear_cache,
    "drain_node": drain_node,
    "page_secondary": page_secondary,
    "no_action": no_action,
}
