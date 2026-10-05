CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE incidents (
  id            UUID PRIMARY KEY,
  fingerprint   TEXT NOT NULL,
  service       TEXT NOT NULL,
  severity      TEXT NOT NULL,
  status        TEXT NOT NULL,          -- open|triaging|awaiting_approval|remediating|verifying|resolved|dead
  title         TEXT NOT NULL,
  alert_count   INT NOT NULL DEFAULT 1,
  first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at   TIMESTAMPTZ,
  hypothesis    JSONB,
  embedding     VECTOR(384),            -- for "have we seen this before?"
  mttr_seconds  INT
);
CREATE UNIQUE INDEX ux_incident_open ON incidents (fingerprint)
  WHERE status <> 'resolved';

CREATE TABLE alerts (
  id BIGSERIAL PRIMARY KEY,
  incident_id UUID REFERENCES incidents(id),
  raw JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE evidence (
  id BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,                   -- metrics|logs|deploys|k8s|similar_incident
  summary TEXT NOT NULL,
  raw JSONB,
  collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE actions (                  -- the audit log; never deleted
  id BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  action TEXT NOT NULL,
  params JSONB NOT NULL,
  tier INT NOT NULL,                    -- 1 read-only, 2 reversible, 3 destructive
  proposed_by TEXT NOT NULL DEFAULT 'sentinel',
  approved_by TEXT,
  dry_run_output JSONB,
  status TEXT NOT NULL,                 -- proposed|approved|rejected|executing|succeeded|failed
  idempotency_key TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE runbooks (
  id BIGSERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  chunk TEXT NOT NULL,
  embedding VECTOR(384)
);
CREATE INDEX ix_runbooks_emb ON runbooks USING hnsw (embedding vector_cosine_ops);

CREATE TABLE suppressions (
  id BIGSERIAL PRIMARY KEY,
  match JSONB NOT NULL,                 -- e.g. {"service":"orders"}
  reason TEXT,
  until TIMESTAMPTZ NOT NULL
);