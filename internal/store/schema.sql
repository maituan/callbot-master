-- Schema for callbot-master persistence layer.
-- Mirrors the v1 layout (freeswitch_adapter/deploy/create.py) so existing
-- ops dashboards / queries continue to work.

CREATE TABLE IF NOT EXISTS call_history (
    call_id       TEXT PRIMARY KEY,
    direction     TEXT NOT NULL,                 -- inbound | outbound (added in v2)
    scenario      TEXT NOT NULL,
    phone         TEXT NOT NULL,
    lead_id       TEXT,
    gender        TEXT,
    name          TEXT,
    plate         TEXT,
    start_time    TIMESTAMPTZ NOT NULL,
    end_time      TIMESTAMPTZ NOT NULL,
    duration_sec  INT GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (end_time - start_time))) STORED,
    status        TEXT NOT NULL DEFAULT 'ended', -- ended | failed | aborted
    action        TEXT,                          -- last bot action: CHAT | ENDCALL
    history       JSONB NOT NULL DEFAULT '[]',   -- per-turn records
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_history_phone_time
    ON call_history (phone, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_call_history_scenario_time
    ON call_history (scenario, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_call_history_direction_time
    ON call_history (direction, start_time DESC);
