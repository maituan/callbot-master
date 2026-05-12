// Package store persists call records to PostgreSQL.
//
// One row per ended call in `call_history`. Per-turn detail lives in the
// `history` JSONB column so we can evolve the turn schema without ALTER
// TABLE migrations.
package store

import (
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/bot"
)

// CallRecord is one persisted call's row. Fields map directly to columns
// in call_history; pgx scans them by struct-tag-less positional read in
// queries.go so any change here must be reflected there.
type CallRecord struct {
	CallID       string    `json:"call_id"`
	Direction    string    `json:"direction"` // inbound | outbound
	Scenario     string    `json:"scenario"`
	Phone        string    `json:"phone"`
	LeadID       string    `json:"lead_id,omitempty"`
	Gender       string    `json:"gender,omitempty"`
	Name         string    `json:"name,omitempty"`
	// Note: the call_history.plate DB column is left intact for
	// back-compat with earlier deployments, but no longer surfaced on
	// the struct. Domain-specific lead fields go through CustomData on
	// campaign.Lead and end up as bot chan vars, not stored columns.
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	DurationSec  int       `json:"duration_sec"` // computed by Postgres
	Status       string    `json:"status"`       // ended | failed | aborted
	Action       string    `json:"action,omitempty"`
	History      []Turn    `json:"history"`
	ErrorMessage string    `json:"error_message,omitempty"`
	RecordingURL string    `json:"recording_url,omitempty"`
	TenantID     *uuid.UUID `json:"tenant_id,omitempty"`
	BotID        *uuid.UUID `json:"bot_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// Turn is one user→bot exchange. Greeting turn has UserText="".
type Turn struct {
	Index     int        `json:"index"`
	UserText  string     `json:"user_text,omitempty"`
	BotText   string     `json:"bot_text"`
	Action    bot.Action `json:"action"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   time.Time  `json:"ended_at"`
	BargedIn  bool       `json:"barged_in,omitempty"`
	// ASRFinalAt — the moment the user "stopped speaking" (ASR is_final).
	// Nil for the greeting turn.
	// FirstAudioAt — the moment the caller first heard a TTS audio frame
	// for this turn. Nil if no audio was produced (e.g. barge-in fired
	// before TTS started).
	// UI computes wait_ms = FirstAudioAt - ASRFinalAt to show how long
	// the caller waited after they stopped speaking.
	ASRFinalAt   *time.Time `json:"asr_final_at,omitempty"`
	FirstAudioAt *time.Time `json:"first_audio_at,omitempty"`
}

// ListFilter narrows GET /api/v1/calls results. Empty fields = no constraint.
type ListFilter struct {
	// Phone is the legacy single-value filter (used by the existing
	// list endpoint + tests). Phones is the multi-value variant the
	// report export wires through. When both are set, Phones wins.
	Phone     string
	Phones    []string
	Scenario  string
	Direction string
	Since     time.Time
	Until     time.Time
	Limit     int
	Offset    int
	// TenantID, when non-nil, restricts the query to one tenant. Set by
	// the API layer based on the caller's identity (tenant_user) and
	// left nil for platform_admin.
	TenantID *uuid.UUID
}
