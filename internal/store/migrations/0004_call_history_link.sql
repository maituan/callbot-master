-- 0004: link call_history to tenant + bot for multi-tenant filtering.
-- tenant_id is RESTRICT so we never silently break call history when a
-- tenant is removed. bot_id SET NULL: deleting a bot keeps history.

ALTER TABLE call_history
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS bot_id    UUID REFERENCES bots(id)    ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_call_history_tenant_time
    ON call_history (tenant_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_call_history_bot_time
    ON call_history (bot_id, start_time DESC);
