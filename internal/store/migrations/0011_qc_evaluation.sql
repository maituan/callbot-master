-- 0011: inline QC for call_history. Carried over from the standalone
-- bot_evaluation tool, trimmed to the bits we need:
--
--   - per-call like/dislike verdict with mandatory reason on dislike
--   - one verdict per call (no editing — audit integrity)
--   - evaluator role gate on `users` so QC isn't every-user-by-default
--
-- v1 covers phone calls (call_history) only. Web sessions get the same
-- panel later via a parallel qc_web_evaluation table or by extending
-- this one with an exclusive web_session_id column.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_evaluator BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS qc_evaluation (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    call_id         TEXT NOT NULL REFERENCES call_history(call_id) ON DELETE CASCADE,
    -- Snapshot tenant at insert so a tenant_user listing only sees
    -- their own evaluations even if the underlying call moves tenant.
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    evaluator_id    UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    verdict         TEXT NOT NULL CHECK (verdict IN ('like','dislike')),
    reason          TEXT,
    -- Dislike requires a reason ≥10 chars (trimmed) so analytics
    -- aren't drowned in "x" / "1" placeholders.
    CHECK (
        verdict = 'like'
        OR (reason IS NOT NULL AND length(btrim(reason)) >= 10)
    ),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (call_id)
);

CREATE INDEX IF NOT EXISTS idx_qc_evaluation_call      ON qc_evaluation (call_id);
CREATE INDEX IF NOT EXISTS idx_qc_evaluation_evaluator ON qc_evaluation (evaluator_id);
CREATE INDEX IF NOT EXISTS idx_qc_evaluation_tenant_t  ON qc_evaluation (tenant_id, created_at DESC);
