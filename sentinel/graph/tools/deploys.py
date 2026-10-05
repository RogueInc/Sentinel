"""
sentinel/graph/tools/deploys.py

Correlates incident onset against recent deploys — GitHub PRs and k8s
rollout history. Usually the single highest-signal piece of evidence.
"""
import asyncio
import json
from datetime import datetime
import httpx
from sentinel.config import settings


async def _recent_prs(service: str, since: datetime) -> list[dict]:
    if not settings.github_repo:
        return []
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"https://api.github.com/repos/{settings.github_repo}/pulls",
            params={"state": "closed", "base": "main", "sort": "updated", "direction": "desc", "per_page": 10},
            headers={"Authorization": f"Bearer {settings.github_token}"} if settings.github_token else {},
        )
        if r.status_code != 200:
            return []
        prs = r.json()
    out = []
    for pr in prs:
        merged_at = pr.get("merged_at")
        if not merged_at:
            continue
        merged_dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
        if merged_dt < since:
            continue
        out.append({
            "kind": "deploy", "source": "github_pr",
            "at": merged_at, "title": pr["title"], "author": pr["user"]["login"],
            "url": pr["html_url"],
        })
    return out


async def _rollout_history(service: str) -> list[dict]:
    """Shells out to kubectl. Returns [] gracefully if kubectl/cluster
    isn't available yet — deploy correlation degrades to PR history only."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "kubectl", "rollout", "history", f"deployment/{service}", "-o", "json",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return []
        data = json.loads(stdout)
        return [{"kind": "deploy", "source": "k8s_rollout", "revision": rev} for rev in data.get("items", [])]
    except FileNotFoundError:
        return []


async def recent_changes(service: str, since: datetime) -> list[dict]:
    """Public entrypoint used by the 'deploys' branch of gather_evidence."""
    prs, rollouts = await asyncio.gather(_recent_prs(service, since), _rollout_history(service))
    return prs + rollouts


def summarize(changes: list[dict]) -> str:
    if not changes:
        return "no deploys or merged PRs found in the lookback window"
    parts = []
    for c in changes[:3]:
        if c.get("source") == "github_pr":
            parts.append(f"PR '{c['title']}' by {c['author']} merged at {c['at']}")
        else:
            parts.append(f"k8s rollout revision {c.get('revision', {}).get('revision', '?')}")
    return "; ".join(parts)
