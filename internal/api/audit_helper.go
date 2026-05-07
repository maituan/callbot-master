package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// AuditWriter is the slice of *store.Postgres mutate handlers use.
// Defined in this file so api packages don't import the full store.
type AuditWriter interface {
	WriteAudit(ctx context.Context, in store.AuditWriteInput) error
}

// recordAudit fires-and-forgets an audit entry. Use ctx.Background-flavored
// timeout to keep the audit write decoupled from the request that triggered it
// (the original ctx may be canceled before the goroutine settles).
//
// We log on failure but never propagate: a busted audit insert is much less
// important than completing the user's mutation.
func recordAudit(w AuditWriter, r *http.Request, tenantID *uuid.UUID, action, entityType, entityID string, before, after any) {
	if w == nil {
		return
	}
	id := auth.FromContext(r.Context())
	in := store.AuditWriteInput{
		TenantID:   tenantID,
		Action:     action,
		EntityType: entityType,
		EntityID:   entityID,
		Before:     before,
		After:      after,
		RequestIP:  clientIP(r),
		UserAgent:  r.UserAgent(),
	}
	if id != nil {
		in.ActorUserID = &id.UserID
		in.ActorUsername = id.Username
		in.ActorRole = id.Role
	}
	go func() {
		if err := w.WriteAudit(context.Background(), in); err != nil {
			slog.Warn("audit write failed", "action", action, "entity", entityType, "err", err)
		}
	}()
}

// clientIP picks the most reliable source-IP value off the request:
// X-Forwarded-For first hop, then RemoteAddr stripped of port. Empty
// is fine — the column is nullable.
func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		return strings.TrimSpace(v)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}
	return host
}
