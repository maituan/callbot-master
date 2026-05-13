// Package intent classifies the user's transcript into a coarse intent
// label so the filler pipeline can pick a long vs short reaction.
//
// The current upstream returns plain text:
//
//	POST <url>  Content-Type: application/json  Accept: text/plain
//	{"conversation_id":"<uuid>", "message":"<transcript>"}
//
//	→ 200 text/plain  "BUSINESS"  or  "CHITCHAT"
//
// Master maps BUSINESS → KindLong and CHITCHAT → KindShort. Anything
// else (timeout, network error, unknown response body, empty URL) is a
// soft fallback to KindShort — the pipeline never blocks on intent.
package intent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Kind mirrors filler.Kind one-to-one. Defining it here separately
// keeps the intent package free of the filler import (would otherwise
// be a circular dependency when filler grows).
type Kind string

const (
	KindShort Kind = "short"
	KindLong  Kind = "long"
)

// ErrNoIntent signals the caller hasn't configured an intent endpoint
// (empty URL). Pipeline treats it as "use short".
var ErrNoIntent = errors.New("intent client not configured")

// Client classifies a transcript into a filler Kind. Implementations
// must honour ctx cancellation so the pipeline can race against the
// bot's first sentence.
type Client interface {
	Classify(ctx context.Context, conversationID, message string) (Kind, error)
}

// HTTPClient hits the configured `url` per call. Stateless aside from
// the inner http.Client (which keeps idle connections warm).
type HTTPClient struct {
	url string
	c   *http.Client
}

// NewHTTPClient builds a client. Empty url makes every Classify call
// return ErrNoIntent immediately so the pipeline takes the fallback
// path without contacting anyone.
func NewHTTPClient(url string) *HTTPClient {
	return &HTTPClient{
		url: url,
		c: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
	}
}

type classifyRequest struct {
	ConversationID string `json:"conversation_id"`
	Message        string `json:"message"`
}

func (c *HTTPClient) Classify(ctx context.Context, conversationID, message string) (Kind, error) {
	if c == nil || c.url == "" {
		return KindShort, ErrNoIntent
	}
	body, err := json.Marshal(classifyRequest{ConversationID: conversationID, Message: message})
	if err != nil {
		return KindShort, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return KindShort, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/plain")

	resp, err := c.c.Do(req)
	if err != nil {
		return KindShort, fmt.Errorf("intent request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return KindShort, fmt.Errorf("intent read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return KindShort, fmt.Errorf("intent status=%d body=%q", resp.StatusCode, raw)
	}

	return ParseLabel(string(raw))
}

// ParseLabel normalises the upstream label. Exposed so tests + dry-run
// CLIs can poke at the mapping without spinning up an HTTP server.
//
// Accepted forms (case-insensitive, surrounding whitespace + quotes
// stripped): BUSINESS → long, CHITCHAT → short. Anything else returns
// KindShort + a non-nil error so callers can log unexpected payloads.
func ParseLabel(raw string) (Kind, error) {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, "\"'")
	s = strings.ToUpper(s)
	switch s {
	case "BUSINESS":
		return KindLong, nil
	case "CHITCHAT":
		return KindShort, nil
	}
	return KindShort, fmt.Errorf("unknown intent label %q", raw)
}
