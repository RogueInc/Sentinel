"""
scratch.py — Phase 3 smoke test. Run from the repo root with your venv
active: `python scratch.py`

Confirms the graph compiles and takes one full pass using an in-memory
checkpointer (not Postgres — that's what worker/runner.py uses for real).

Prerequisites:
- Postgres running with migrations/001_init.sql AND 002_events.sql applied
- .env present in the repo root with at least POSTGRES_DSN set
- LLM_PROVIDER_URL can stay blank — nodes fall back to canned JSON

This does NOT need Redis, the worker, or the API — it calls build_graph()
directly, bypassing the queue entirely.
"""
import asyncio
import uuid

from langgraph.checkpoint.memory import MemorySaver

from sentinel.db import execute
from sentinel.graph.build import build_graph


async def seed_test_incident() -> str:
    """Inserts a throwaway incident row so classify()/postmortem() have
    something real to read/write against."""
    incident_id = str(uuid.uuid4())
    await execute(
        """
        INSERT INTO incidents (id, fingerprint, service, severity, status, title)
        VALUES ($1, $2, 'orders', 'warning', 'open', 'scratch.py smoke test')
        """,
        incident_id, f"scratch-{incident_id}",
    )
    return incident_id


async def main():
    incident_id = await seed_test_incident()
    print(f"seeded incident {incident_id}")

    graph = build_graph(checkpointer=MemorySaver())
    state = {
        "incident_id": incident_id,
        "service": "orders",
        "severity": "warning",
        "alert": {},
        "dig_count": 0,
        "verify_count": 0,
    }
    cfg = {"configurable": {"thread_id": incident_id}}

    async for event in graph.astream(state, cfg, stream_mode="updates"):
        for node, output in event.items():
            print(f"\n--- {node} ---")
            print(output)

    snapshot = await graph.aget_state(cfg)
    if snapshot.next:
        print(f"\nGraph paused before: {snapshot.next} (this is correct if propose_action chose a tier-2+ action)")
    else:
        print("\nGraph reached END.")


if __name__ == "__main__":
    asyncio.run(main())