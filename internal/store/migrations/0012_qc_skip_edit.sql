-- 0012: QC v2 — skip verdict + editable evaluations.
--
-- Changes from 0011:
--   1. verdict CHECK widens to include 'skipped' — for calls the
--      reviewer chose to bypass (no useful content, hangup, etc).
--      Skip doesn't require a reason.
--   2. Reason CHECK only constrains 'dislike' now.
--   3. Add updated_at + last_updated_by so re-evaluating a call
--      overwrites the row but keeps the original evaluator_id (audit
--      trail), with the audit_log capturing every change.
--
-- The qc_evaluation table itself stays UNIQUE on call_id; the
-- application layer upserts instead of inserting blindly.

-- Drop CHECK constraints by introspection (PG auto-named them).
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT conname FROM pg_constraint
         WHERE conrelid = 'qc_evaluation'::regclass
           AND contype  = 'c'
    LOOP
        EXECUTE format('ALTER TABLE qc_evaluation DROP CONSTRAINT %I', r.conname);
    END LOOP;
END $$;

ALTER TABLE qc_evaluation
    ADD CONSTRAINT qc_evaluation_verdict_chk
        CHECK (verdict IN ('like','dislike','skipped'));

ALTER TABLE qc_evaluation
    ADD CONSTRAINT qc_evaluation_reason_chk
        CHECK (
            verdict <> 'dislike'
            OR (reason IS NOT NULL AND length(btrim(reason)) >= 10)
        );

ALTER TABLE qc_evaluation
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE qc_evaluation
    ADD COLUMN IF NOT EXISTS last_updated_by UUID
        REFERENCES users(id) ON DELETE RESTRICT;
