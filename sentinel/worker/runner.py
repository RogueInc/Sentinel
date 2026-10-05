"""
sentinel/worker/runner.py

The main worker loop: pulls one incident at a time from the priority
queue, runs (or resumes) the LangGraph triage graph with thread_id =
incident_id, streams progress into incident_events for SSE, and acks
only once the graph reaches a terminal state or pauses at interrupt().

Graceful shutdown: SIGTERM stops the poll loop and lets any in-flight
graph invocation finish (checkpointed, so nothing is lost even if it
doesn't finish in time).
"""
import asyncio
import json
import logging
import signal

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.types import Command

from sentinel.config import settings
from sentinel.db import execute, fetchrow
from sentinel.graph.build import build_graph
from sentinel.queue.consumer import ack, consume_one, ensure_group
from sentinel.queue.dlq import handle_failure
from sentinel.queue.reclaim import reclaim_loop

log = logging.getLogger("sentinel.worker")

_shutdown = asyncio.Event()


def _handle_sigterm(*_):
    log.info("SIGTERM received, draining...")
    _shutdown.set()


async def _load_initial_state(incident_id: str) -> dict:
    row = await fetchrow(
        "SELECT id, service, severity FROM incidents WHERE id = $1", incident_id
    )
    return {
        "incident_id": incident_id,
        "service": row["service"],
        "severity": row["severity"],
        "alert": {},
        "dig_count": 0,
        "verify_count": 0,
    }


async def emit_event(incident_id: str, kind: str, data: dict):
    await execute(
        """
        INSERT INTO incident_events (incident_id, seq, kind, data)
        VALUES ($1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM incident_events WHERE incident_id = $1), $2, $3)
        """,
        incident_id, kind, json.dumps(data, default=str),
    )


async def process_message(graph, stream: str, msg_id: str, fields: dict):
    from sentinel.obs.tracing import tracer, extract_context
    from sentinel.obs.metrics import TRIAGE_DURATION, QUEUE_DEPTH
    import time

    incident_id = fields["incident_id"]
    resume_payload = fields.get("resume")
    cfg = {"configurable": {"thread_id": incident_id}}
    ctx = extract_context(fields)
    start = time.monotonic()

    with tracer.start_as_current_span("incident.triage", context=ctx) as span:
        span.set_attribute("incident.id", incident_id)
        try:
            await execute("UPDATE incidents SET status = 'triaging' WHERE id = $1", incident_id)

            if resume_payload:
                run_input = Command(resume=json.loads(resume_payload))
            else:
                run_input = await _load_initial_state(incident_id)

            async for event in graph.astream(run_input, cfg, stream_mode="updates"):
                for node_name, node_output in event.items():
                    await emit_event(incident_id, "node_update", {"node": node_name, "output": node_output})

            snapshot = await graph.aget_state(cfg)
            if snapshot.next:
                await execute("UPDATE incidents SET status = 'awaiting_approval' WHERE id = $1", incident_id)
                await emit_event(incident_id, "paused", {"next": list(snapshot.next)})
                outcome = "paused"
            else:
                await emit_event(incident_id, "completed", {})
                outcome = "completed"

            await ack(stream, msg_id)
            severity_row = await fetchrow("SELECT severity FROM incidents WHERE id = $1", incident_id)
            TRIAGE_DURATION.labels(severity_row["severity"] if severity_row else "unknown", outcome).observe(
                time.monotonic() - start
            )

        except Exception as e:
            log.exception("incident %s failed", incident_id)
            span.record_exception(e)
            await emit_event(incident_id, "error", {"error": str(e)})
            await handle_failure(stream, msg_id, fields, str(e))


async def run_worker():
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    from sentinel.obs.tracing import init_tracing
    init_tracing()

    await ensure_group(settings.stream_p1)
    await ensure_group(settings.stream_p2)

    async with AsyncPostgresSaver.from_conn_string(settings.postgres_dsn) as checkpointer:
        await checkpointer.setup()
        graph = build_graph(checkpointer=checkpointer)

        reclaim_task = asyncio.create_task(reclaim_loop(graph, process_message))

        log.info("worker started, consumer=%s", settings.consumer_group)
        while not _shutdown.is_set():
            msg = await consume_one(block_ms=2000)
            if msg is None:
                continue
            stream, msg_id, fields = msg
            await process_message(graph, stream, msg_id, fields)

        reclaim_task.cancel()
        log.info("worker drained, exiting")