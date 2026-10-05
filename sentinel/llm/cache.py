"""
sentinel/llm/cache.py

Exact-match cache keyed by sha256(model|prompt), plus a semantic
fallback via pgvector cosine distance for near-duplicate prompts.
Only ever used for temperature=0 calls — see llm/client.py — since a
sampled response shouldn't be treated as reusable.
"""
import hashlib
import json

from sentinel.db import fetchrow, execute
from sentinel.config import settings
from sentinel.graph.tools.runbook_search import embed  # reuses the same embedding model


def _exact_key(prompt: str, model: str) -> str:
    return hashlib.sha256(f"{model}|{prompt}".encode()).hexdigest()


async def get(prompt: str, model: str) -> dict | None:
    key = _exact_key(prompt, model)
    row = await fetchrow("SELECT response FROM llm_cache WHERE key = $1", key)
    if row:
        await execute("UPDATE llm_cache SET hits = hits + 1 WHERE key = $1", key)
        return json.loads(row["response"])

    vec = embed(prompt)
    row = await fetchrow(
        """
        SELECT response, (embedding <=> $1) AS distance
        FROM llm_cache
        ORDER BY embedding <=> $1
        LIMIT 1
        """,
        vec,
    )
    if row and row["distance"] < settings.semantic_cache_threshold:
        return json.loads(row["response"])
    return None


async def put(prompt: str, model: str, response: dict):
    key = _exact_key(prompt, model)
    vec = embed(prompt)
    await execute(
        """
        INSERT INTO llm_cache (key, embedding, response, hits)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (key) DO UPDATE SET response = EXCLUDED.response
        """,
        key, vec, json.dumps(response),
    )