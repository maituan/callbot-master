// Package store persists call records to PostgreSQL.
//
// One row per ended call in `call_history`. Per-turn detail lives in the
// `history` JSONB column so we can evolve the turn schema without ALTER
// TABLE migrations.
package store

import (
	"time"

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
	Plate        string    `json:"plate,omitempty"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	DurationSec  int       `json:"duration_sec"` // computed by Postgres
	Status       string    `json:"status"`       // ended | failed | aborted
	Action       string    `json:"action,omitempty"`
	History      []Turn    `json:"history"`
	ErrorMessage string    `json:"error_message,omitempty"`
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
}

// ListFilter narrows GET /api/v1/calls results. Empty fields = no constraint.
type ListFilter struct {
	Phone     string
	Scenario  string
	Direction string
	Since     time.Time
	Until     time.Time
	Limit     int
	Offset    int
}
