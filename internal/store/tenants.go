package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Tenant struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// GetTenantBySlug returns nil,nil when not found.
func (p *Postgres) GetTenantBySlug(ctx context.Context, slug string) (*Tenant, error) {
	const q = `SELECT id, slug, name, enabled, created_at, updated_at FROM tenants WHERE slug = $1`
	row := p.pool.QueryRow(ctx, q, slug)
	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

func (p *Postgres) GetTenantByID(ctx context.Context, id uuid.UUID) (*Tenant, error) {
	const q = `SELECT id, slug, name, enabled, created_at, updated_at FROM tenants WHERE id = $1`
	row := p.pool.QueryRow(ctx, q, id)
	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

func (p *Postgres) ListTenants(ctx context.Context) ([]*Tenant, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, slug, name, enabled, created_at, updated_at FROM tenants ORDER BY slug`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// CreateTenant inserts a new tenant. Returns ErrSlugTaken if the slug
// is already in use (UNIQUE constraint).
func (p *Postgres) CreateTenant(ctx context.Context, slug, name string) (uuid.UUID, error) {
	var id uuid.UUID
	err := p.pool.QueryRow(ctx,
		`INSERT INTO tenants (slug, name) VALUES ($1, $2) RETURNING id`,
		slug, name).Scan(&id)
	if err != nil && isUniqueViolation(err) {
		return uuid.Nil, ErrSlugTaken
	}
	return id, err
}

// UpdateTenant changes the name + enabled flag. Slug is immutable
// because it's referenced from bots.tenant_id and changing it would
// invalidate all bookmarks/links to it.
func (p *Postgres) UpdateTenant(ctx context.Context, id uuid.UUID, name string, enabled bool) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE tenants SET name = $1, enabled = $2 WHERE id = $3`,
		name, enabled, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteTenant hard-deletes a tenant. The bots+users FK is RESTRICT, so
// this errors out if anything still references it — the API layer
// translates that to a 409 with a "remove dependents first" hint.
func (p *Postgres) DeleteTenant(ctx context.Context, id uuid.UUID) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		if isFKViolation(err) {
			return ErrTenantHasDependents
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ErrSlugTaken / ErrNotFound / ErrTenantHasDependents are sentinels the
// API uses to map DB errors to HTTP status codes.
var (
	ErrSlugTaken          = errors.New("slug already in use")
	ErrNotFound           = errors.New("not found")
	ErrTenantHasDependents = errors.New("tenant still has bots or users")
)

// isFKViolation matches Postgres SQLSTATE 23503 (foreign_key_violation).
func isFKViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23503")
}

// UpsertTenant is used by the bootstrap path to ensure the seed tenant
// (e.g. "hcc") exists. Updates name on conflict so renames stick.
func (p *Postgres) UpsertTenant(ctx context.Context, slug, name string) (uuid.UUID, error) {
	const q = `
INSERT INTO tenants (slug, name) VALUES ($1, $2)
ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
RETURNING id`
	var id uuid.UUID
	if err := p.pool.QueryRow(ctx, q, slug, name).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("upsert tenant: %w", err)
	}
	return id, nil
}

func scanTenant(row scannable) (*Tenant, error) {
	var t Tenant
	if err := row.Scan(&t.ID, &t.Slug, &t.Name, &t.Enabled, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	return &t, nil
}
