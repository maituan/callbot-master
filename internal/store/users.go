package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Role values mirror the DB CHECK constraint on users.role.
const (
	RolePlatformAdmin = "platform_admin"
	RoleTenantUser    = "tenant_user"
)

// User is the persisted login identity. PasswordHash is bcrypt; never
// expose it across the API surface.
type User struct {
	ID           uuid.UUID
	Username     string
	PasswordHash string
	Role         string
	TenantID     *uuid.UUID // nil for platform_admin
	Enabled      bool
	LastLoginAt  *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// GetUserByUsername returns nil,nil when not found (callers distinguish
// missing vs. error without importing pgx.ErrNoRows).
func (p *Postgres) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	const q = `
SELECT id, username, password_hash, role, tenant_id, enabled,
       last_login_at, created_at, updated_at
FROM users WHERE username = $1`
	row := p.pool.QueryRow(ctx, q, username)
	u, err := scanUser(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return u, err
}

func (p *Postgres) GetUserByID(ctx context.Context, id uuid.UUID) (*User, error) {
	const q = `
SELECT id, username, password_hash, role, tenant_id, enabled,
       last_login_at, created_at, updated_at
FROM users WHERE id = $1`
	row := p.pool.QueryRow(ctx, q, id)
	u, err := scanUser(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return u, err
}

// UpsertPlatformAdmin keeps the bootstrap admin in sync with env.
// On every startup we re-hash the configured password — handy if the
// operator rotates it via env without touching SQL.
func (p *Postgres) UpsertPlatformAdmin(ctx context.Context, username, passwordHash string) error {
	const q = `
INSERT INTO users (username, password_hash, role, tenant_id, enabled)
VALUES ($1, $2, 'platform_admin', NULL, true)
ON CONFLICT (username) DO UPDATE SET
    password_hash = EXCLUDED.password_hash,
    role          = 'platform_admin',
    tenant_id     = NULL,
    enabled       = true`
	_, err := p.pool.Exec(ctx, q, username, passwordHash)
	if err != nil {
		return fmt.Errorf("upsert platform_admin: %w", err)
	}
	return nil
}

// CreateTenantUser inserts a new tenant_user. Returns the row's UUID.
// Caller must verify the actor's role + the tenant exists.
func (p *Postgres) CreateTenantUser(ctx context.Context, username, passwordHash string, tenantID uuid.UUID) (uuid.UUID, error) {
	const q = `
INSERT INTO users (username, password_hash, role, tenant_id, enabled)
VALUES ($1, $2, 'tenant_user', $3, true)
RETURNING id`
	var id uuid.UUID
	if err := p.pool.QueryRow(ctx, q, username, passwordHash, tenantID).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("create tenant_user: %w", err)
	}
	return id, nil
}

// UpdateUserPassword resets a user's password. Only callable by the user
// themselves (self-service) or by a platform_admin.
func (p *Postgres) UpdateUserPassword(ctx context.Context, userID uuid.UUID, passwordHash string) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE users SET password_hash = $1 WHERE id = $2`, passwordHash, userID)
	return err
}

// MarkLogin updates last_login_at. Best-effort — failures shouldn't
// block the actual login response.
func (p *Postgres) MarkLogin(ctx context.Context, userID uuid.UUID) {
	_, _ = p.pool.Exec(ctx,
		`UPDATE users SET last_login_at = now() WHERE id = $1`, userID)
}

// ListUsers returns users in scope. tenantID nil = every user (admin).
// platform_admin rows always come back regardless of scope so an admin
// looking at another tenant's view still sees themselves.
func (p *Postgres) ListUsers(ctx context.Context, tenantID *uuid.UUID) ([]*User, error) {
	q := `
SELECT id, username, password_hash, role, tenant_id, enabled,
       last_login_at, created_at, updated_at
FROM users`
	args := []any{}
	if tenantID != nil {
		q += " WHERE tenant_id = $1 OR role = 'platform_admin'"
		args = append(args, *tenantID)
	}
	q += " ORDER BY role, username"
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// UpdateUserEnabled toggles the enabled flag. Disabled users keep
// existing JWTs valid until expiry — clients have to log out + back in.
// In practice that's fine for our 12h TTL.
func (p *Postgres) UpdateUserEnabled(ctx context.Context, id uuid.UUID, enabled bool) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE users SET enabled = $1 WHERE id = $2`, enabled, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteUser hard-deletes a user. Refuses if the user is the only
// platform_admin (we'd lock ourselves out).
func (p *Postgres) DeleteUser(ctx context.Context, id uuid.UUID) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var role string
	if err := tx.QueryRow(ctx, `SELECT role FROM users WHERE id = $1`, id).Scan(&role); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if role == RolePlatformAdmin {
		var n int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM users WHERE role = 'platform_admin'`).Scan(&n); err != nil {
			return err
		}
		if n <= 1 {
			return ErrLastAdmin
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM users WHERE id = $1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ErrLastAdmin is returned when an operation would leave the system
// with no platform_admin (lockout protection).
var ErrLastAdmin = errors.New("cannot delete or demote the last platform admin")

func scanUser(row scannable) (*User, error) {
	var u User
	if err := row.Scan(
		&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.TenantID,
		&u.Enabled, &u.LastLoginAt, &u.CreatedAt, &u.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &u, nil
}
