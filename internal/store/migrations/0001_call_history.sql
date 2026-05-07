-- 0001: original call_history schema (extracted from schema.sql).
-- Idempotent so first-time installs and existing deployments converge.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS call_history (
    call_id       TEXT PRIMARY KEY,
    direction     TEXT NOT NULL,
    scenario      TEXT NOT NULL,
    phone         TEXT NOT NULL,
    lead_id       TEXT,
    gender        TEXT,
    name          TEXT,
    plate         TEXT,
    start_time    TIMESTAMPTZ NOT NULL,
    end_time      TIMESTAMPTZ NOT NULL,
    duration_sec  INT GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (end_time - start_time))) STORED,
    status        TEXT NOT NULL DEFAULT 'ended',
    action        TEXT,
    history       JSONB NOT NULL DEFAULT '[]',
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_url TEXT;

CREATE INDEX IF NOT EXISTS idx_call_history_phone_time
    ON call_history (phone, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_call_history_scenario_time
    ON call_history (scenario, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_call_history_direction_time
    ON call_history (direction, start_time DESC);
