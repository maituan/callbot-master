// Package intent classifies the user's transcript into a coarse intent
// label so the filler pipeline can pick a matching reaction audio.
//
// The upstream returns a plain-text label, e.g.:
//
//	POST <url>  Content-Type: application/json  Accept: text/plain
//	{"conversation_id":"<uuid>", "message":"<transcript>"}
//
//	→ 200 text/plain  "PROCEDURE_NEW"  (or PROCEDURE_FIELD_REASK,
//	   META, OFF_TOPIC, CHITCHAT, … — the set is open-ended)
//
// Master treats the label as an opaque string: the filler player maps
// it to <voice>/<LABEL>/*.wav and falls back to the flat short pool
// when no folder matches. That keeps adding/renaming intents a
// data-only change (drop in an audio folder) with zero code edits.
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

// ErrNoIntent signals the caller hasn't configured an intent endpoint
// (empty URL). Pipeline treats it as "use the fallback short pool".
var ErrNoIntent = errors.New("intent client not configured")

// Client classifies a transcript into an intent label. Implementations
// must honour ctx cancellation so the pipeline can bound the wait.
type Client interface {
	Classify(ctx context.Context, conversationID, message string) (string, error)
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

// Classify returns the normalised label (see NormalizeLabel). On any
// failure it returns ("", err) so the caller can fall back to the
// short pool without inspecting the error type.
func (c *HTTPClient) Classify(ctx context.Context, conversationID, message string) (string, error) {
	if c == nil || c.url == "" {
		return "", ErrNoIntent
	}
	body, err := json.Marshal(classifyRequest{ConversationID: conversationID, Message: message})
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/plain")

	resp, err := c.c.Do(req)
	if err != nil {
		return "", fmt.Errorf("intent request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", fmt.Errorf("intent read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("intent status=%d body=%q", resp.StatusCode, raw)
	}

	label := NormalizeLabel(string(raw))
	if label == "" {
		return "", fmt.Errorf("intent returned empty label (body=%q)", raw)
	}
	return label, nil
}

// NormalizeLabel trims surrounding whitespace + quotes and uppercases
// the result so "  procedure_new\n" and "\"PROCEDURE_NEW\"" both map to
// the same folder name. Returns "" for an all-whitespace body.
//
// Exposed so the filler player + tests share one canonicalisation rule
// — folders must be named with the normalised (UPPERCASE) form.
func NormalizeLabel(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, "\"'")
	s = strings.TrimSpace(s)
	return strings.ToUpper(s)
}
