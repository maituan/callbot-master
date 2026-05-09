package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// WebSession is one playground conversation (chat or voice) opened by an
// anonymous visitor via a bot-share token.
type WebSession struct {
	ID            uuid.UUID
	BotID         uuid.UUID
	TenantID      uuid.UUID
	Channel       string // "chat" | "voice"
	TokenIAT      *time.Time
	IP            string // text form, "" when unknown
	UserAgent     string
	StartedAt     time.Time
	EndedAt       *time.Time
	Status        string // "active" | "ended" | "aborted" | "error"
	TotalTurns    int
	RecordingDir  string // empty when not recording or chat
	ErrorMessage  string
	// Turns is hydrated by GetWebSession. List queries leave it nil to
	// keep the payload small.
	Turns []*WebTurn
}

type WebTurn struct {
	ID               uuid.UUID
	SessionID        uuid.UUID
	Idx              int
	Role             string // "user" | "bot"
	Text             string
	AudioPath        string
	ASRPartialAt     *time.Time
	ASRFinalAt       *time.Time
	BotFirstByteAt   *time.Time
	BotDoneAt        *time.Time
	TTSFirstAudioAt  *time.Time
	TTSDoneAt        *time.Time
	Action           string
	CreatedAt        time.Time
}

// CreateWebSession inserts a new web_session and returns the generated id.
// Caller passes started_at = time.Now() implicitly via DEFAULT.
func (p *Postgres) CreateWebSession(ctx context.Context, s *WebSession) error {
	if s.BotID == uuid.Nil || s.TenantID == uuid.Nil {
		return errors.New("bot_id and tenant_id required")
	}
	if s.Channel != "chat" && s.Channel != "voice" {
		return fmt.Errorf("invalid channel: %s", s.Channel)
	}
	const q = `
INSERT INTO web_session (bot_id, tenant_id, channel, token_iat, ip, user_agent, recording_dir, status)
VALUES ($1, $2, $3, $4, NULLIF($5,'')::inet, NULLIF($6,''), NULLIF($7,''), COALESCE(NULLIF($8,''), 'active'))
RETURNING id, started_at, status`
	row := p.pool.QueryRow(ctx, q,
		s.BotID, s.TenantID, s.Channel, s.TokenIAT,
		safeIP(s.IP), s.UserAgent, s.RecordingDir, s.Status,
	)
	return row.Scan(&s.ID, &s.StartedAt, &s.Status)
}

// safeIP strips the port from a "host:port" remote-addr string and
// validates the host is a real IP (so an X-Forwarded-For garbage value
// doesn't crash the inet cast).
func safeIP(s string) string {
	if s == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(s); err == nil {
		s = h
	}
	if net.ParseIP(s) == nil {
		return ""
	}
	return s
}

// EndWebSession marks the session ended (or aborted/error) and stamps
// ended_at. Idempotent — repeated calls overwrite status/error.
func (p *Postgres) EndWebSession(ctx context.Context, id uuid.UUID, status, errMsg string) error {
	if status == "" {
		status = "ended"
	}
	const q = `
UPDATE web_session
   SET ended_at = COALESCE(ended_at, now()),
       status = $2,
       error_message = NULLIF($3, '')
 WHERE id = $1`
	_, err := p.pool.Exec(ctx, q, id, status, errMsg)
	return err
}

// AppendWebTurn inserts one turn and bumps web_session.total_turns. The
// idx is computed on the server side from the current count to avoid
// race conditions when multiple turns are appended concurrently (which
// shouldn't happen per session but is cheap insurance).
func (p *Postgres) AppendWebTurn(ctx context.Context, t *WebTurn) error {
	if t.SessionID == uuid.Nil || t.Role == "" {
		return errors.New("session_id and role required")
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if t.Idx == 0 {
		var cur int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(idx),0)+1 FROM web_turn WHERE session_id = $1`,
			t.SessionID,
		).Scan(&cur); err != nil {
			return fmt.Errorf("compute idx: %w", err)
		}
		t.Idx = cur
	}

	const q = `
INSERT INTO web_turn (
  session_id, idx, role, text, audio_path,
  asr_partial_at, asr_final_at,
  bot_first_byte_at, bot_done_at,
  tts_first_audio_at, tts_done_at,
  action
) VALUES (
  $1,$2,$3,$4,NULLIF($5,''),
  $6,$7,
  $8,$9,
  $10,$11,
  NULLIF($12,'')
) RETURNING id, created_at`
	if err := tx.QueryRow(ctx, q,
		t.SessionID, t.Idx, t.Role, t.Text, t.AudioPath,
		t.ASRPartialAt, t.ASRFinalAt,
		t.BotFirstByteAt, t.BotDoneAt,
		t.TTSFirstAudioAt, t.TTSDoneAt,
		t.Action,
	).Scan(&t.ID, &t.CreatedAt); err != nil {
		return fmt.Errorf("insert turn: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE web_session SET total_turns = (SELECT COUNT(*) FROM web_turn WHERE session_id=$1) WHERE id=$1`,
		t.SessionID,
	); err != nil {
		return fmt.Errorf("bump total_turns: %w", err)
	}
	return tx.Commit(ctx)
}

// WebSessionFilter narrows ListWebSessions. BotID is required for
// tenant-scoped views; the API layer enforces tenant ownership before
// calling here.
type WebSessionFilter struct {
	BotID    uuid.UUID
	TenantID uuid.UUID
	Channel  string // "" | "chat" | "voice"
	Status   string // "" | active | ended | aborted | error
	Limit    int
	Offset   int
}

// ListWebSessions returns sessions ordered by started_at DESC. The Turns
// field is left nil — call GetWebSession for the full body.
func (p *Postgres) ListWebSessions(ctx context.Context, f WebSessionFilter) ([]*WebSession, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	args := []any{}
	conds := []string{"1=1"}
	addCond := func(expr string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(expr, len(args)))
	}
	if f.BotID != uuid.Nil {
		addCond("bot_id = $%d", f.BotID)
	}
	if f.TenantID != uuid.Nil {
		addCond("tenant_id = $%d", f.TenantID)
	}
	if f.Channel != "" {
		addCond("channel = $%d", f.Channel)
	}
	if f.Status != "" {
		addCond("status = $%d", f.Status)
	}
	args = append(args, f.Limit, f.Offset)
	q := `
SELECT id, bot_id, tenant_id, channel, token_iat, host(ip), COALESCE(user_agent,''),
       started_at, ended_at, status, total_turns, COALESCE(recording_dir,''), COALESCE(error_message,'')
FROM web_session
WHERE ` + joinAnd(conds) + `
ORDER BY started_at DESC
LIMIT $` + itoa(len(args)-1) + ` OFFSET $` + itoa(len(args))

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*WebSession{}
	for rows.Next() {
		s := &WebSession{}
		var ip string
		if err := rows.Scan(
			&s.ID, &s.BotID, &s.TenantID, &s.Channel, &s.TokenIAT, &ip, &s.UserAgent,
			&s.StartedAt, &s.EndedAt, &s.Status, &s.TotalTurns, &s.RecordingDir, &s.ErrorMessage,
		); err != nil {
			return nil, err
		}
		s.IP = ip
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetWebSession returns one session including its turns ordered by idx.
func (p *Postgres) GetWebSession(ctx context.Context, id uuid.UUID) (*WebSession, error) {
	const sq = `
SELECT id, bot_id, tenant_id, channel, token_iat, host(ip), COALESCE(user_agent,''),
       started_at, ended_at, status, total_turns, COALESCE(recording_dir,''), COALESCE(error_message,'')
FROM web_session WHERE id = $1`
	row := p.pool.QueryRow(ctx, sq, id)
	s := &WebSession{}
	var ip string
	if err := row.Scan(
		&s.ID, &s.BotID, &s.TenantID, &s.Channel, &s.TokenIAT, &ip, &s.UserAgent,
		&s.StartedAt, &s.EndedAt, &s.Status, &s.TotalTurns, &s.RecordingDir, &s.ErrorMessage,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	s.IP = ip

	const tq = `
SELECT id, session_id, idx, role, text, COALESCE(audio_path,''),
       asr_partial_at, asr_final_at, bot_first_byte_at, bot_done_at,
       tts_first_audio_at, tts_done_at, COALESCE(action,''), created_at
FROM web_turn WHERE session_id = $1 ORDER BY idx`
	rows, err := p.pool.Query(ctx, tq, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		t := &WebTurn{}
		if err := rows.Scan(
			&t.ID, &t.SessionID, &t.Idx, &t.Role, &t.Text, &t.AudioPath,
			&t.ASRPartialAt, &t.ASRFinalAt, &t.BotFirstByteAt, &t.BotDoneAt,
			&t.TTSFirstAudioAt, &t.TTSDoneAt, &t.Action, &t.CreatedAt,
		); err != nil {
			return nil, err
		}
		s.Turns = append(s.Turns, t)
	}
	return s, rows.Err()
}

func joinAnd(xs []string) string {
	out := ""
	for i, s := range xs {
		if i > 0 {
			out += " AND "
		}
		out += s
	}
	return out
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }
