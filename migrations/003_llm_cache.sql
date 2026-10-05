-- Required by sentinel/llm/cache.py (Phase 5's semantic + exact LLM
-- response cache). Never included in 001_init.sql for this project —
-- apply after 001_init.sql and 002_events.sql.

CREATE TABLE llm_cache (
  key        TEXT PRIMARY KEY,
  embedding  VECTOR(384),
  response   JSONB NOT NULL,
  hits       INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_llm_cache_emb ON llm_cache USING hnsw (embedding vector_cosine_ops);