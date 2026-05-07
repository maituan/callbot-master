package pipeline

import (
	"errors"
	"time"

	"callbot-master/internal/bot"
	"callbot-master/internal/store"
)

// buildCallRecord assembles the persistence row from the runner's opts and
// the pipeline's accumulated history. p may be nil if Run failed before
// the pipeline was constructed (e.g. mkfifo error) — the record still
// captures opts + status so ops can see infrastructure failures.
func buildCallRecord(opts RunOpts, p *Pipeline, runErr error, endedAt time.Time) *store.CallRecord {
	rec := &store.CallRecord{
		CallID:    opts.UUID,
		Direction: opts.Direction.String(),
		Scenario:  opts.Scenario,
		Phone:     opts.Phone,
		LeadID:    opts.LeadID,
		Gender:    opts.Gender,
		Name:      opts.Name,
		Plate:     opts.Plate,
		StartTime: opts.StartedAt,
		EndTime:   endedAt,
	}

	switch {
	case runErr == nil:
		rec.Status = "ended"
	case errors.Is(runErr, errCanceled{}):
		rec.Status = "aborted"
	default:
		rec.Status = "failed"
		rec.ErrorMessage = runErr.Error()
	}

	if p != nil {
		hist := p.History()
		rec.History = make([]store.Turn, len(hist))
		var lastAction bot.Action
		for i, t := range hist {
			rec.History[i] = store.Turn{
				Index:     t.Index,
				UserText:  t.UserText,
				BotText:   t.BotText,
				Action:    t.Action,
				StartedAt: t.StartedAt,
				EndedAt:   t.EndedAt,
				BargedIn:  t.BargedIn,
			}
			lastAction = t.Action
		}
		rec.Action = string(lastAction)
	} else {
		rec.History = []store.Turn{}
	}
	return rec
}

// errCanceled is a sentinel used so buildCallRecord can recognize the "ctx
// canceled" path without importing context just for the comparison. Today
// it's unused beyond the type-switch hook above; left as a no-op to keep
// the API symmetric for future expansion.
type errCanceled struct{}

func (errCanceled) Error() string { return "canceled" }
