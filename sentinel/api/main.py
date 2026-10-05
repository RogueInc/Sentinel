"""sentinel/api/main.py — FastAPI app entrypoint."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from sentinel.ingest.routes import router as ingest_router
from sentinel.api.routes_incidents import router as incidents_router
from sentinel.api.sse import router as sse_router
from sentinel.obs.tracing import init_tracing

app = FastAPI(title="Sentinel")
init_tracing(app)

app.include_router(ingest_router)
app.include_router(incidents_router)
app.include_router(sse_router)

_UI_PATH = Path(__file__).parent / "ui" / "index.html"


@app.get("/")
def ui():
    # no-store: this file changes frequently during development, and
    # FileResponse's default Last-Modified/ETag headers let browsers
    # silently keep serving a stale cached copy across normal refreshes.
    return FileResponse(_UI_PATH, headers={"Cache-Control": "no-store"})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    from fastapi.responses import Response
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)