-- 0005: append-only audit log of mutations + auth events.
-- No FKs on actor/tenant: audit must remain even after the entity is deleted.

CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       UUID,
    actor_user_id   UUID,
    actor_username  TEXT,
    actor_role      TEXT,
    action          TEXT NOT NULL,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT,
    before          JSONB,
    after           JSONB,
    request_ip      INET,
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_tenant_time
    ON audit_log (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity
    ON audit_log (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor
    ON audit_log (actor_user_id);
