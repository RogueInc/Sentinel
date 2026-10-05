-- Not in the original Phase 2 schema, but required by worker/runner.py's
-- emit_event() and api/sse.py's stream endpoint. Apply after 001_init.sql.

CREATE TABLE incident_events (
  id          BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
  seq         INT  NOT NULL,
  kind        TEXT NOT NULL,      -- node_update|paused|completed|error
  data        JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_incident_events_lookup ON incident_events (incident_id, seq);