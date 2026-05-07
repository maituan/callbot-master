// Package bot defines the streaming bot REST contract.
// Default impl (callbot-hcc-base-ts /api/v1/call/) lives in http_stream.go (Phase 1).
//
// Wire format reminder (see CLAUDE.md "Bot API contract"):
//   - text/plain chunked transfer
//   - sentence-flush on . ? ! … \n
//   - last segment after final "|" carries the action: CHAT | ENDCALL
package bot

import "context"

type Action string

const (
	ActionChat    Action = "CHAT"
	ActionEndCall Action = "ENDCALL"
)

// Client streams a single turn of the conversation.
// conversationID = FreeSWITCH call uuid (single source of truth).
type Client interface {
	Stream(ctx context.Context, conversationID, message string) (TurnStream, error)
}

// TurnStream emits sentences as they're flushed from the bot's chunked
// response, then exposes the parsed action once the HTTP stream closes.
//
// Lifecycle:
//   1. Caller ranges over Sentences() until the channel closes.
//   2. After Sentences() closes, Action() returns the parsed action.
//   3. Caller MUST call Close() if it abandons the stream early.
type TurnStream interface {
	Sentences() <-chan string
	Action() (Action, error)
	Close() error
}
