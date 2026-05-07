package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
)

// AuditEntry is one append-only record. Actor* fields are SNAPSHOTS at
// the time of the action — even if the user is later deleted, we still
// know who did what.
type AuditEntry struct {
	ID            int64
	TenantID      *uuid.UUID
	ActorUserID   *uuid.UUID
	ActorUsername string
	ActorRole     string
	Action        string // 'bot.create' | 'bot.update' | 'bot.delete' | 'did.add' | …
	EntityType    string // 'bot' | 'tenant' | 'user' | 'did'
	EntityID      string
	Before        json.RawMessage
	After         json.RawMessage
	RequestIP     *net.IP
	UserAgent     string
	CreatedAt     time.Time
}

// AuditWriteInput is the call-site shape — pointers omitted for the
// few fields that mostly aren't set, to keep mutate handlers clean.
type AuditWriteInput struct {
	TenantID      *uuid.UUID
	ActorUserID   *uuid.UUID
	ActorUsername string
	ActorRole     string
	Action        string
	EntityType    string
	EntityID      string
	Before        any // marshalled to jsonb; nil → SQL NULL
	After         any
	RequestIP     string // empty → NULL
	UserAgent     string
}

// WriteAudit inserts one audit row. Best-effort — callers ignore
// errors so a failed audit doesn't reject the underlying mutation;
// failures are logged elsewhere if anyone cares.
func (p *Postgres) WriteAudit(ctx context.Context, in AuditWriteInput) error {
	beforeJSON, err := encodeAudit(in.Before)
	if err != nil {
		return fmt.Errorf("marshal before: %w", err)
	}
	afterJSON, err := encodeAudit(in.After)
	if err != nil {
		return fmt.Errorf("marshal after: %w", err)
	}
	var ip any
	if in.RequestIP != "" {
		ip = in.RequestIP
	}
	const q = `
INSERT INTO audit_log (
    tenant_id, actor_user_id, actor_username, actor_role,
    action, entity_type, entity_id,
    before, after, request_ip, user_agent
) VALUES ($1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$11)`
	_, err = p.pool.Exec(ctx, q,
		in.TenantID, in.ActorUserID, nullStr(in.ActorUsername), nullStr(in.ActorRole),
		in.Action, in.EntityType, nullStr(in.EntityID),
		beforeJSON, afterJSON, ip, nullStr(in.UserAgent))
	return err
}

func encodeAudit(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// AuditFilter narrows GET /api/v1/audit. Empty fields = no constraint.
type AuditFilter struct {
	TenantID    *uuid.UUID
	ActorUserID *uuid.UUID
	EntityType  string
	EntityID    string
	Action      string
	Since       time.Time
	Until       time.Time
	Limit       int
	Offset      int
}

// ListAudit returns rows ordered by created_at DESC. Caller is
// responsible for tenant scope (TenantID nil = unscoped).
func (p *Postgres) ListAudit(ctx context.Context, filter AuditFilter) ([]*AuditEntry, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	q := `
SELECT id, tenant_id, actor_user_id, COALESCE(actor_username,''), COALESCE(actor_role,''),
       action, entity_type, COALESCE(entity_id,''),
       COALESCE(before, 'null'::jsonb), COALESCE(after, 'null'::jsonb),
       request_ip, COALESCE(user_agent,''), created_at
FROM audit_log`
	args := []any{}
	conds := []string{}
	add := func(cond string, val any) {
		conds = append(conds, fmt.Sprintf(cond, len(args)+1))
		args = append(args, val)
	}
	if filter.TenantID != nil {
		add("tenant_id = $%d", *filter.TenantID)
	}
	if filter.ActorUserID != nil {
		add("actor_user_id = $%d", *filter.ActorUserID)
	}
	if filter.EntityType != "" {
		add("entity_type = $%d", filter.EntityType)
	}
	if filter.EntityID != "" {
		add("entity_id = $%d", filter.EntityID)
	}
	if filter.Action != "" {
		add("action = $%d", filter.Action)
	}
	if !filter.Since.IsZero() {
		add("created_at >= $%d", filter.Since)
	}
	if !filter.Until.IsZero() {
		add("created_at < $%d", filter.Until)
	}
	if len(conds) > 0 {
		q += " WHERE "
		for i, c := range conds {
			if i > 0 {
				q += " AND "
			}
			q += c
		}
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
	args = append(args, limit, filter.Offset)

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AuditEntry
	for rows.Next() {
		var e AuditEntry
		var ip *net.IP
		var beforeRaw, afterRaw []byte
		if err := rows.Scan(
			&e.ID, &e.TenantID, &e.ActorUserID, &e.ActorUsername, &e.ActorRole,
			&e.Action, &e.EntityType, &e.EntityID,
			&beforeRaw, &afterRaw,
			&ip, &e.UserAgent, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		e.Before = json.RawMessage(beforeRaw)
		e.After = json.RawMessage(afterRaw)
		e.RequestIP = ip
		out = append(out, &e)
	}
	return out, rows.Err()
}
