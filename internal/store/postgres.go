package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the concrete impl backed by pgx v5.
type Postgres struct {
	pool *pgxpool.Pool
}

// New connects to dsn and applies the schema. Returns an error if the
// connection or migration fails. Caller must Close on shutdown.
//
// dsn format: "postgres://user:pass@host:5432/db?sslmode=disable" or
// any other libpq-compatible string pgx accepts.
func New(ctx context.Context, dsn string) (*Postgres, error) {
	if dsn == "" {
		return nil, errors.New("dsn is empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	if err := applyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Pool exposes the underlying pgxpool.Pool for adjacent stores
// (users, tenants, bots) that share the same DB connection.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// Close releases the pool. Idempotent.
func (p *Postgres) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// Insert upserts a call record. Reusing INSERT … ON CONFLICT lets us
// idempotently re-persist if the session goroutine retries.
func (p *Postgres) Insert(ctx context.Context, r *CallRecord) error {
	historyJSON, err := json.Marshal(r.History)
	if err != nil {
		return fmt.Errorf("marshal history: %w", err)
	}
	const q = `
INSERT INTO call_history (
    call_id, direction, scenario, phone,
    lead_id, gender, name,
    start_time, end_time, status, action, history, error_message, recording_url,
    tenant_id, bot_id
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9, $10, $11, $12, $13, $14,
    $15, $16
)
ON CONFLICT (call_id) DO UPDATE SET
    end_time      = EXCLUDED.end_time,
    status        = EXCLUDED.status,
    action        = EXCLUDED.action,
    history       = EXCLUDED.history,
    error_message = EXCLUDED.error_message,
    -- Don't blank an existing recording_url with NULL on UPSERT — the
    -- archiver writes it AFTER the initial Insert, in a separate UPDATE.
    recording_url = COALESCE(EXCLUDED.recording_url, call_history.recording_url),
    tenant_id     = COALESCE(EXCLUDED.tenant_id, call_history.tenant_id),
    bot_id        = COALESCE(EXCLUDED.bot_id, call_history.bot_id);
`
	_, err = p.pool.Exec(ctx, q,
		r.CallID, r.Direction, r.Scenario, r.Phone,
		nullStr(r.LeadID), nullStr(r.Gender), nullStr(r.Name),
		r.StartTime, r.EndTime, r.Status, nullStr(r.Action),
		historyJSON, nullStr(r.ErrorMessage), nullStr(r.RecordingURL),
		r.TenantID, r.BotID,
	)
	return err
}

// SetRecordingURL is the post-archive hook the recording archiver calls
// once the MP3 has been copied to the persistent dir. Separate from
// Insert because the archive runs asynchronously after end-of-call.
func (p *Postgres) SetRecordingURL(ctx context.Context, callID, url string) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE call_history SET recording_url = $1 WHERE call_id = $2`,
		url, callID)
	return err
}

// Get returns the call by id. Returns (nil, nil) when not found so callers
// distinguish "not found" from "query error" without sql.ErrNoRows imports.
func (p *Postgres) Get(ctx context.Context, callID string) (*CallRecord, error) {
	const q = `
SELECT call_id, direction, scenario, phone,
       COALESCE(lead_id,''), COALESCE(gender,''), COALESCE(name,''),
       start_time, end_time, duration_sec,
       status, COALESCE(action,''),
       history, COALESCE(error_message,''), COALESCE(recording_url,''),
       tenant_id, bot_id, created_at
FROM call_history
WHERE call_id = $1
`
	row := p.pool.QueryRow(ctx, q, callID)
	r, err := scanRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// List returns up to filter.Limit records (default 50, max 500) ordered
// by start_time DESC.
func (p *Postgres) List(ctx context.Context, filter ListFilter) ([]*CallRecord, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	var (
		conds []string
		args  []any
		i     = 1
	)
	add := func(cond string, val any) {
		conds = append(conds, fmt.Sprintf(cond, i))
		args = append(args, val)
		i++
	}
	if filter.Phone != "" {
		add("phone = $%d", filter.Phone)
	}
	if filter.Scenario != "" {
		add("scenario = $%d", filter.Scenario)
	}
	if filter.Direction != "" {
		add("direction = $%d", filter.Direction)
	}
	if !filter.Since.IsZero() {
		add("start_time >= $%d", filter.Since)
	}
	if !filter.Until.IsZero() {
		add("start_time < $%d", filter.Until)
	}
	if filter.TenantID != nil {
		add("tenant_id = $%d", *filter.TenantID)
	}

	q := strings.Builder{}
	q.WriteString(`
SELECT call_id, direction, scenario, phone,
       COALESCE(lead_id,''), COALESCE(gender,''), COALESCE(name,''),
       start_time, end_time, duration_sec,
       status, COALESCE(action,''),
       history, COALESCE(error_message,''), COALESCE(recording_url,''),
       tenant_id, bot_id, created_at
FROM call_history
`)
	if len(conds) > 0 {
		q.WriteString("WHERE " + strings.Join(conds, " AND ") + "\n")
	}
	q.WriteString(fmt.Sprintf("ORDER BY start_time DESC LIMIT $%d OFFSET $%d", i, i+1))
	args = append(args, limit, filter.Offset)

	rows, err := p.pool.Query(ctx, q.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []*CallRecord
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// scanRow centralizes column ordering so Get/List stay in sync.
type scannable interface {
	Scan(dest ...any) error
}

func scanRow(row scannable) (*CallRecord, error) {
	var r CallRecord
	var historyJSON []byte
	if err := row.Scan(
		&r.CallID, &r.Direction, &r.Scenario, &r.Phone,
		&r.LeadID, &r.Gender, &r.Name,
		&r.StartTime, &r.EndTime, &r.DurationSec,
		&r.Status, &r.Action,
		&historyJSON, &r.ErrorMessage, &r.RecordingURL,
		&r.TenantID, &r.BotID, &r.CreatedAt,
	); err != nil {
		return nil, err
	}
	if len(historyJSON) > 0 {
		if err := json.Unmarshal(historyJSON, &r.History); err != nil {
			return nil, fmt.Errorf("unmarshal history: %w", err)
		}
	}
	return &r, nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
