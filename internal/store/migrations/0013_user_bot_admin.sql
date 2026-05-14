-- 0013: separate "manage bots" from "log in as tenant_user".
--
-- Until now any tenant_user could PATCH/POST/DELETE bots in their
-- tenant — fine when the same people manage AI config and review
-- calls, but a leak when outsourced QC evaluators only need to rate
-- conversations. Introduce users.is_bot_admin so admins can split
-- the two responsibilities.
--
-- Default TRUE so every existing tenant_user keeps their bot-manage
-- power on day one. Admin can flip individual users to FALSE when
-- onboarding QC-only personnel; the matching is_evaluator flag (from
-- migration 0011) controls QC access independently.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_bot_admin BOOLEAN NOT NULL DEFAULT true;
