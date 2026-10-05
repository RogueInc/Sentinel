"""
sentinel/obs/tracing.py

OTel init + trace-context propagation through Redis (since the queue
message, not an HTTP call, carries the span across the webhook -> queue
-> worker boundary), plus a decorator for wrapping graph nodes as spans.
"""
import functools

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import extract, inject, set_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from sentinel.config import settings

tracer = trace.get_tracer("sentinel")


def init_tracing(app=None):
    # Explicitly force the W3C tracecontext propagator rather than
    # relying on the SDK's entry-point-based default resolution — that
    # resolution can silently fall back to a no-op propagator depending
    # on install order, which was causing inject()/extract() to produce
    # empty traceparent values and every queue hop to start a brand-new,
    # disconnected trace instead of continuing the request's trace.
    set_global_textmap(TraceContextTextMapPropagator())

    provider = TracerProvider(resource=Resource.create({"service.name": settings.service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    if app is not None:
        FastAPIInstrumentor.instrument_app(app)


def inject_context(fields: dict) -> dict:
    """Call when enqueueing, so the worker can pick the trace back up."""
    carrier: dict = {}
    inject(carrier)
    return {**fields, "traceparent": carrier.get("traceparent", "")}


def extract_context(fields: dict):
    return extract({"traceparent": fields.get("traceparent", "")})


def traced_node(name: str):
    """Wrap a graph node function so every invocation is its own span,
    tagged with incident id and token/cost usage from its return value."""
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(state):
            with tracer.start_as_current_span(f"node.{name}") as span:
                span.set_attribute("incident.id", state.get("incident_id", ""))
                out = await fn(state)
                span.set_attribute("llm.tokens", out.get("tokens_used", 0))
                span.set_attribute("llm.cost_usd", out.get("cost_usd", 0.0))
                return out
        return wrapper
    return decorator