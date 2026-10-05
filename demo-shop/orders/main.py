import asyncio
import logging
import random

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)   # exposes GET /metrics

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("orders")

CHAOS = {"latency_ms": 0, "error_rate": 0.0, "leak": False}
LEAK: list[bytes] = []


class ChaosConfig(BaseModel):
    latency_ms: int | None = None
    error_rate: float | None = None
    leak: bool | None = None


@app.post("/chaos")
def chaos(cfg: ChaosConfig):
    CHAOS.update(cfg.model_dump(exclude_none=True))
    return CHAOS


@app.get("/orders/{oid}")
async def get_order(oid: str):
    if CHAOS["latency_ms"]:
        await asyncio.sleep(CHAOS["latency_ms"] / 1000)
    if CHAOS["leak"]:
        LEAK.append(bytes(5_000_000))
    if random.random() < CHAOS["error_rate"]:
        log.error("db_pool_exhausted service=orders order_id=%s", oid)
        raise HTTPException(500, "connection pool exhausted")
    return {"id": oid, "status": "shipped"}


@app.get("/health")
def health():
    return {"status": "ok"}