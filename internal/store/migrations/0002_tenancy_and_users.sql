-- 0002: tenants + users.
-- platform_admin = users.tenant_id IS NULL.
-- tenant_user    = users.tenant_id IS NOT NULL (must be a real tenant).

CREATE TABLE IF NOT EXISTS tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug        TEXT NOT NULL UNIQUE
                  CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$'),
    name        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username        TEXT NOT NULL UNIQUE
                      CHECK (length(username) BETWEEN 3 AND 64),
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL
                      CHECK (role IN ('platform_admin','tenant_user')),
    tenant_id       UUID REFERENCES tenants(id) ON DELETE RESTRICT,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_role_tenant CHECK (
        (role = 'platform_admin' AND tenant_id IS NULL) OR
        (role = 'tenant_user'    AND tenant_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_users_tenant
    ON users (tenant_id) WHERE tenant_id IS NOT NULL;
