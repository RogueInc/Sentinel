"""
sentinel/graph/tools/runbook_search.py

pgvector search over runbooks + resolved past incidents. Requires the
`sentence-transformers` embedding model and the `runbooks`/`incidents`
tables from migrations/001_init.sql.
"""
from sentence_transformers import SentenceTransformer
from sentinel.db import fetch

_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def embed(text: str) -> list[float]:
    return _get_model().encode(text).tolist()


async def similar_incidents(query_text: str, k: int = 3) -> list[dict]:
    vec = embed(query_text)
    rows = await fetch(
        """
        SELECT id, title, hypothesis, mttr_seconds, (embedding <=> $1) AS distance
        FROM incidents
        WHERE status = 'resolved' AND embedding IS NOT NULL
        ORDER BY embedding <=> $1
        LIMIT $2
        """,
        vec, k,
    )
    return [dict(r) for r in rows]


async def matching_runbooks(query_text: str, k: int = 3) -> list[dict]:
    vec = embed(query_text)
    rows = await fetch(
        """
        SELECT id, title, chunk, (embedding <=> $1) AS distance
        FROM runbooks
        ORDER BY embedding <=> $1
        LIMIT $2
        """,
        vec, k,
    )
    return [dict(r) for r in rows]
