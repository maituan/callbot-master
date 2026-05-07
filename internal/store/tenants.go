package store

import (
	"context"
	"errors"
	"fmt"
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
