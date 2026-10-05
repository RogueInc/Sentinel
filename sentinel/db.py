"""
Thin async Postgres helper built on asyncpg.
pip install asyncpg pgvector
"""
import asyncpg
from pgvector.asyncpg import register_vector
from contextlib import asynccontextmanager
from sentinel.config import settings

_pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection):
    """Registers the pgvector codec so asyncpg can encode/decode Python
    lists as the `vector` column type. Without this, any query passing
    an embedding (postmortem.py, runbook_search.py, llm/cache.py) fails
    with 'expected str, got list'."""
    await register_vector(conn)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=2, max_size=10, init=_init_connection
        )
    return _pool


@asynccontextmanager
async def tx():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn


async def fetchrow(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def execute(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)