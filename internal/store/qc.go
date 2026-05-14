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

// QCEvaluation is a single human verdict on one phone call.
type QCEvaluation struct {
	ID          uuid.UUID
	CallID      string
	TenantID    uuid.UUID
	EvaluatorID uuid.UUID
	// EvaluatorName is hydrated by GetQCEvaluation for display — the
	// CRUD insert ignores it.
	EvaluatorName string
	Verdict       string // "like" | "dislike" | "skipped"
	Reason        string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	// LastUpdatedBy is the most recent reviewer to touch the row; may
	// differ from EvaluatorID (original creator) when another reviewer
	// later overwrites the verdict.
	LastUpdatedBy *uuid.UUID
}

// QCWriteInput is the user-facing slice of QCEvaluation: id +
// EvaluatorName are server-generated and tenant_id is resolved from
// the call.
type QCWriteInput struct {
	CallID      string
	EvaluatorID uuid.UUID
	Verdict     string
	Reason      string
}

// ErrQCReasonRequired is returned when verdict=dislike but reason is
// missing or too short (<10 chars after trim).
var ErrQCReasonRequired = errors.New("dislike requires a reason of at least 10 characters")

// UpsertQCEvaluation creates or overwrites the verdict for a call.
// The original `evaluator_id` is preserved across edits (audit trail);
// `last_updated_by` captures whoever last touched it. Reason is
// required for verdict='dislike' (≥10 chars after trim), optional for
// 'like', and ignored for 'skipped'.
func (p *Postgres) UpsertQCEvaluation(ctx context.Context, in QCWriteInput) (*QCEvaluation, error) {
	if in.CallID == "" || in.EvaluatorID == uuid.Nil {
		return nil, errors.New("call_id and evaluator_id required")
	}
	switch in.Verdict {
	case "like":
		in.Reason = strings.TrimSpace(in.Reason)
	case "dislike":
		in.Reason = strings.TrimSpace(in.Reason)
		if len(in.Reason) < 10 {
			return nil, ErrQCReasonRequired
		}
	case "skipped":
		// Skipped doesn't carry a reason — clear any stray text.
		in.Reason = ""
	default:
		return nil, fmt.Errorf("invalid verdict: %q", in.Verdict)
	}

	// Snapshot tenant_id from the call.
	var tenantID *uuid.UUID
	if err := p.pool.QueryRow(ctx,
		`SELECT tenant_id FROM call_history WHERE call_id = $1`, in.CallID,
	).Scan(&tenantID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("call not found: %s", in.CallID)
		}
		return nil, fmt.Errorf("lookup call tenant: %w", err)
	}
	if tenantID == nil {
		return nil, fmt.Errorf("call %s has no tenant (legacy row, cannot QC)", in.CallID)
	}

	// Upsert: on conflict, keep the original creator + creation time
	// but bump verdict/reason/updated_at/last_updated_by.
	const q = `
INSERT INTO qc_evaluation
    (call_id, tenant_id, evaluator_id, verdict, reason, last_updated_by)
VALUES ($1, $2, $3, $4, NULLIF($5,''), $3)
ON CONFLICT (call_id) DO UPDATE SET
    verdict         = EXCLUDED.verdict,
    reason          = EXCLUDED.reason,
    updated_at      = now(),
    last_updated_by = EXCLUDED.evaluator_id
RETURNING id, created_at, updated_at`
	out := &QCEvaluation{
		CallID:      in.CallID,
		TenantID:    *tenantID,
		EvaluatorID: in.EvaluatorID,
		Verdict:     in.Verdict,
		Reason:      in.Reason,
	}
	if err := p.pool.QueryRow(ctx, q,
		in.CallID, *tenantID, in.EvaluatorID, in.Verdict, in.Reason,
	).Scan(&out.ID, &out.CreatedAt, &out.UpdatedAt); err != nil {
		return nil, fmt.Errorf("upsert qc_evaluation: %w", err)
	}
	return out, nil
}

// CreateQCEvaluation is kept as a thin alias for back-compat with the
// existing handler — semantics now match UpsertQCEvaluation (last
// writer wins instead of erroring on duplicate).
func (p *Postgres) CreateQCEvaluation(ctx context.Context, in QCWriteInput) (*QCEvaluation, error) {
	return p.UpsertQCEvaluation(ctx, in)
}

// GetQCEvaluationByCallID returns the verdict for one call (with the
// evaluator's display name) or (nil, nil) if not yet evaluated.
func (p *Postgres) GetQCEvaluationByCallID(ctx context.Context, callID string) (*QCEvaluation, error) {
	const q = `
SELECT q.id, q.call_id, q.tenant_id, q.evaluator_id,
       COALESCE(u.username, ''),
       q.verdict, COALESCE(q.reason, ''),
       q.created_at, q.updated_at, q.last_updated_by
FROM qc_evaluation q
JOIN users u ON u.id = q.evaluator_id
WHERE q.call_id = $1`
	out := &QCEvaluation{}
	err := p.pool.QueryRow(ctx, q, callID).Scan(
		&out.ID, &out.CallID, &out.TenantID, &out.EvaluatorID,
		&out.EvaluatorName, &out.Verdict, &out.Reason,
		&out.CreatedAt, &out.UpdatedAt, &out.LastUpdatedBy,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
