-- 0006: shared updated_at trigger for tenants/users/bots.
-- Re-create function so the migration is idempotent.

CREATE OR REPLACE FUNCTION trg_set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tg_tenants_upd ON tenants;
CREATE TRIGGER tg_tenants_upd BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

DROP TRIGGER IF EXISTS tg_users_upd ON users;
CREATE TRIGGER tg_users_upd BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

DROP TRIGGER IF EXISTS tg_bots_upd ON bots;
CREATE TRIGGER tg_bots_upd BEFORE UPDATE ON bots
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();
