package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"callbot-master/internal/filler"
)

// fillerController owns the async decision flow:
//
//   - resolver goroutine (optional): calls Pipeline.FillerKindResolver,
//     emits filler.Kind via decisionCh.
//   - decider goroutine: select on decisionCh, firstSentenceCh, timeout.
//     Whoever wins picks the kind (or skips filler). On a non-skip
//     decision, calls Pipeline.Filler.Play and stores the resulting
//     stop fn.
//
// The main Speak goroutine calls fillerCtrl.Cancel() when it's ready
// to open the TTS sink (first audio frame). Cancel is idempotent:
// stops any started filler + cancels in-flight decision.
type fillerController struct {
	mu      sync.Mutex
	stopFn  func()
	done    chan struct{}
	cancel  context.CancelFunc
	stopped bool
}

// newFillerController kicks off the decision flow. Returns immediately;
// the controller runs in its own goroutine.
//
// active is true only on the lazy-sink + non-empty-message path —
// otherwise we don't want filler at all (greeting / offline tests /
// barge-in interjection) and the controller becomes a no-op that
// still honours Cancel().
func newFillerController(parent context.Context, p *Pipeline, transcript string, active bool, firstSentenceCh <-chan struct{}) *fillerController {
	ctrl := &fillerController{done: make(chan struct{})}

	wanted := active && transcript != "" && p.Cfg.FillerEnabled && p.Filler != nil
	if !wanted {
		close(ctrl.done)
		return ctrl
	}

	ctx, cancel := context.WithCancel(parent)
	ctrl.cancel = cancel

	// Kick off the resolver in its own goroutine so it doesn't
	// piggy-back on the decider's select. The buffered channel makes
	// the resolver fire-and-forget — even if the decider picks
	// firstSentenceCh first, the resolver writes once and exits.
	decisionCh := make(chan filler.Kind, 1)
	resolverWanted := p.FillerKindResolver != nil
	if resolverWanted {
		go func() {
			resCtx := ctx
			if p.Cfg.FillerIntentTimeout > 0 {
				var cancelInner context.CancelFunc
				resCtx, cancelInner = context.WithTimeout(ctx, p.Cfg.FillerIntentTimeout)
				defer cancelInner()
			}
			kind, err := p.FillerKindResolver(resCtx, transcript)
			if err != nil {
				slog.Debug("filler intent resolver", "call_uuid", p.UUID, "err", err)
				kind = filler.KindShort
			}
			select {
			case decisionCh <- kind:
			case <-ctx.Done():
			}
		}()
	}

	go func() {
		defer close(ctrl.done)
		kind := filler.KindShort
		skip := false

		switch {
		case resolverWanted:
			select {
			case k := <-decisionCh:
				kind = k
			case <-firstSentenceCh:
				// Bot is faster than intent — abandon filler so we
				// don't cue audio that gets cut almost immediately.
				skip = true
			case <-time.After(timeoutFloor(p.Cfg.FillerIntentTimeout)):
				// Defensive cap in case the resolver hangs past its
				// own ctx (shouldn't happen but cheap insurance).
			case <-ctx.Done():
				return
			}
		default:
			// No resolver wired → default short. Still race against
			// first-sentence so we don't cue when bot is already
			// answering (rare in this branch but consistent).
			select {
			case <-firstSentenceCh:
				skip = true
			case <-ctx.Done():
				return
			default:
				// Fall through to play immediately.
			}
		}
		if skip {
			return
		}

		stop, ok := p.Filler.Play(ctx, p.UUID, p.Cfg.VoiceID, kind)
		if !ok {
			return
		}
		ctrl.mu.Lock()
		defer ctrl.mu.Unlock()
		if ctrl.stopped {
			// Cancel arrived between Play and the lock — fire the
			// stop fn immediately so we don't leave broadcast playing
			// past the next TTS frame.
			stop()
			return
		}
		ctrl.stopFn = stop
	}()

	return ctrl
}

// Cancel stops any in-flight filler and prevents the controller from
// starting one if the decision hasn't fired yet. Idempotent.
func (c *fillerController) Cancel() {
	if c == nil {
		return
	}
	c.mu.Lock()
	already := c.stopped
	c.stopped = true
	stop := c.stopFn
	c.stopFn = nil
	c.mu.Unlock()
	if already {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	if stop != nil {
		stop()
	}
}

// timeoutFloor returns a safe upper bound used as a defensive cap on
// the decider goroutine. If FillerIntentTimeout is zero (caller wants
// no deadline) we still apply a 5s ceiling so a hung resolver can't
// keep the controller alive forever.
func timeoutFloor(d time.Duration) time.Duration {
	if d <= 0 || d > 5*time.Second {
		return 5 * time.Second
	}
	// Add a small margin so the resolver's own ctx times out first
	// and the decider receives KindShort via decisionCh rather than
	// hitting this catch-all.
	return d + 100*time.Millisecond
}
