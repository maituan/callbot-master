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
	Verdict       string // "like" | "dislike"
	Reason        string
	CreatedAt     time.Time
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

// ErrQCAlreadyEvaluated is returned when a verdict already exists for
// the call — verdicts are one-shot to keep audit integrity.
var ErrQCAlreadyEvaluated = errors.New("call already has an evaluation")

// ErrQCReasonRequired is returned when verdict=dislike but reason is
// missing or too short (<10 chars after trim).
var ErrQCReasonRequired = errors.New("dislike requires a reason of at least 10 characters")

// CreateQCEvaluation inserts a verdict and snapshots the call's
// tenant_id alongside. UNIQUE(call_id) bubbles up as ErrQCAlreadyEvaluated.
func (p *Postgres) CreateQCEvaluation(ctx context.Context, in QCWriteInput) (*QCEvaluation, error) {
	if in.CallID == "" || in.EvaluatorID == uuid.Nil {
		return nil, errors.New("call_id and evaluator_id required")
	}
	switch in.Verdict {
	case "like":
		in.Reason = strings.TrimSpace(in.Reason) // optional praise note
	case "dislike":
		in.Reason = strings.TrimSpace(in.Reason)
		if len(in.Reason) < 10 {
			return nil, ErrQCReasonRequired
		}
	default:
		return nil, fmt.Errorf("invalid verdict: %q", in.Verdict)
	}

	// Snapshot tenant_id from the call. Reject if the call doesn't
	// belong to a tenant (legacy rows pre-tenancy) — those can't be
	// QC-scoped safely.
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

	const q = `
INSERT INTO qc_evaluation (call_id, tenant_id, evaluator_id, verdict, reason)
VALUES ($1, $2, $3, $4, NULLIF($5,''))
RETURNING id, created_at`
	out := &QCEvaluation{
		CallID:      in.CallID,
		TenantID:    *tenantID,
		EvaluatorID: in.EvaluatorID,
		Verdict:     in.Verdict,
		Reason:      in.Reason,
	}
	if err := p.pool.QueryRow(ctx, q,
		in.CallID, *tenantID, in.EvaluatorID, in.Verdict, in.Reason,
	).Scan(&out.ID, &out.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			return nil, ErrQCAlreadyEvaluated
		}
		return nil, fmt.Errorf("insert qc_evaluation: %w", err)
	}
	return out, nil
}

// GetQCEvaluationByCallID returns the verdict for one call (with the
// evaluator's display name) or (nil, nil) if not yet evaluated.
func (p *Postgres) GetQCEvaluationByCallID(ctx context.Context, callID string) (*QCEvaluation, error) {
	const q = `
SELECT q.id, q.call_id, q.tenant_id, q.evaluator_id,
       COALESCE(u.username, ''),
       q.verdict, COALESCE(q.reason, ''), q.created_at
FROM qc_evaluation q
JOIN users u ON u.id = q.evaluator_id
WHERE q.call_id = $1`
	out := &QCEvaluation{}
	err := p.pool.QueryRow(ctx, q, callID).Scan(
		&out.ID, &out.CallID, &out.TenantID, &out.EvaluatorID,
		&out.EvaluatorName, &out.Verdict, &out.Reason, &out.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
