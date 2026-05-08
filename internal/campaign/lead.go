// Package campaign manages outbound dialing campaigns: CSV → leads, a
// CCU-bounded worker pool, per-lead status tracking, cancel support.
//
// Decoupled from FreeSWITCH and the bot stack — the originate side is
// injected as an OriginateFunc. That keeps the manager testable without
// any audio infrastructure.
package campaign

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// CallStatus is the per-lead state across the dialing lifecycle.
type CallStatus string

const (
	StatusPending   CallStatus = "pending"
	StatusDialing   CallStatus = "dialing"
	StatusAnswered  CallStatus = "answered"
	StatusCompleted CallStatus = "completed"
	StatusFailed    CallStatus = "failed"
	StatusNoAnswer  CallStatus = "no_answer"
	StatusCanceled  CallStatus = "canceled"
)

// Lead is one row from the CSV plus runtime tracking fields. Domain-
// specific columns (e.g. "plate", "policy_id", "amount") live in
// CustomData and are forwarded to the bot as chan vars; only the four
// fields below are recognised explicitly because they map to columns
// on call_history.
type Lead struct {
	Phone      string                 `json:"phone"`
	LeadID     string                 `json:"lead_id,omitempty"`
	Gender     string                 `json:"gender,omitempty"`
	Name       string                 `json:"name,omitempty"`
	CustomData map[string]any         `json:"custom_data,omitempty"`
	Status     CallStatus             `json:"status"`
	CallUUID   string                 `json:"call_uuid,omitempty"`
	Error      string                 `json:"error,omitempty"`
	StartedAt  *time.Time             `json:"started_at,omitempty"`
	EndedAt    *time.Time             `json:"ended_at,omitempty"`
}

// ParseCSV reads a CSV with a header row. The "phone" column is required.
// Recognized columns map to fields on Lead; everything else lands in
// CustomData (forwarded as FreeSWITCH chan vars at originate time).
//
// Whitespace in cell values is trimmed; empty cells skipped. Header names
// are lowercased and trimmed for matching.
func ParseCSV(r io.Reader) ([]*Lead, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	phoneIdx := -1
	for i, h := range header {
		header[i] = strings.TrimSpace(strings.ToLower(h))
		if header[i] == "phone" {
			phoneIdx = i
		}
	}
	if phoneIdx < 0 {
		return nil, fmt.Errorf("CSV must have a 'phone' column")
	}

	// Well-known columns mapped directly to Lead fields. Anything else
	// (incl. domain-specific columns like "plate", "policy_id" …) goes
	// into CustomData so the bot adapter can still see it.
	known := map[string]bool{
		"phone": true, "lead_id": true, "gender": true, "name": true,
	}

	var leads []*Lead
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}

		l := &Lead{Status: StatusPending, CustomData: map[string]any{}}
		for i, val := range row {
			if i >= len(header) {
				break
			}
			val = strings.TrimSpace(val)
			if val == "" {
				continue
			}
			col := header[i]
			switch col {
			case "phone":
				l.Phone = val
			case "lead_id":
				l.LeadID = val
			case "gender":
				l.Gender = val
			case "name":
				l.Name = val
			default:
				if !known[col] {
					l.CustomData[col] = val
				}
			}
		}
		if l.Phone != "" {
			leads = append(leads, l)
		}
	}
	return leads, nil
}

// Stats is a campaign-level rollup of lead statuses.
type Stats struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Dialing   int `json:"dialing"`
	Answered  int `json:"answered"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	NoAnswer  int `json:"no_answer"`
	Canceled  int `json:"canceled"`
}

// Campaign holds leads + status. mu guards Leads slice and Status string.
type Campaign struct {
	ID        string    `json:"id"`
	Scenario  string    `json:"scenario"`
	CallerID  string    `json:"caller_id"`
	CCU       int       `json:"ccu"`
	Leads     []*Lead   `json:"leads"`
	Status    string    `json:"status"` // "running" | "done" | "canceled"
	CreatedAt time.Time `json:"created_at"`

	mu       sync.Mutex
	canceled bool
}

// Stats returns a snapshot of lead-status counts. O(n) over leads.
func (c *Campaign) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	var s Stats
	s.Total = len(c.Leads)
	for _, l := range c.Leads {
		switch l.Status {
		case StatusPending:
			s.Pending++
		case StatusDialing:
			s.Dialing++
		case StatusAnswered:
			s.Answered++
		case StatusCompleted:
			s.Completed++
		case StatusFailed:
			s.Failed++
		case StatusNoAnswer:
			s.NoAnswer++
		case StatusCanceled:
			s.Canceled++
		}
	}
	return s
}

// FindByUUID returns the lead matching callUUID, or nil if not found.
func (c *Campaign) FindByUUID(callUUID string) *Lead {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.Leads {
		if l.CallUUID == callUUID {
			return l
		}
	}
	return nil
}

// SetLeadStatus mutates the lead identified by callUUID. EndedAt is set
// automatically for terminal states.
func (c *Campaign) SetLeadStatus(callUUID string, status CallStatus, errMsg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.Leads {
		if l.CallUUID != callUUID {
			continue
		}
		l.Status = status
		if errMsg != "" {
			l.Error = errMsg
		}
		if isTerminal(status) {
			now := time.Now()
			l.EndedAt = &now
		}
		return
	}
}

func isTerminal(s CallStatus) bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusNoAnswer, StatusCanceled:
		return true
	}
	return false
}
