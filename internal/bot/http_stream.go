package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// HTTPClient implements bot.Client against the Hono streaming endpoint
// (callbot-hcc-base-ts /api/v1/call/). One client = many turns.
type HTTPClient struct {
	url              string
	firstByteTimeout time.Duration
	totalTimeout     time.Duration
	client           *http.Client
}

// NewHTTPClient configures the underlying transport. firstByteTimeout maps to
// Transport.ResponseHeaderTimeout; totalTimeout is enforced via per-request
// context deadline (so streaming reads can flow but the whole turn caps).
func NewHTTPClient(url string, firstByte, total time.Duration) *HTTPClient {
	return &HTTPClient{
		url:              url,
		firstByteTimeout: firstByte,
		totalTimeout:     total,
		client: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: firstByte,
				MaxIdleConns:          32,
				MaxIdleConnsPerHost:   16,
				IdleConnTimeout:       90 * time.Second,
				// We rely on chunked transfer; disable any compression that
				// would buffer the response.
				DisableCompression: true,
			},
		},
	}
}

type callRequest struct {
	ConversationID string `json:"conversation_id"`
	Message        string `json:"message"`
}

func (c *HTTPClient) Stream(parent context.Context, conversationID, message string) (TurnStream, error) {
	body, err := json.Marshal(callRequest{ConversationID: conversationID, Message: message})
	if err != nil {
		return nil, fmt.Errorf("marshal call request: %w", err)
	}

	ctx, cancel := context.WithTimeout(parent, c.totalTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/plain")

	resp, err := c.client.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("bot request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("bot status=%d body=%q", resp.StatusCode, b)
	}

	s := &httpTurnStream{
		ctx:            ctx,
		cancel:         cancel,
		resp:           resp,
		sentences:      make(chan string, 8),
		done:           make(chan struct{}),
		conversationID: conversationID,
	}
	go s.read()
	return s, nil
}

type httpTurnStream struct {
	ctx            context.Context
	cancel         context.CancelFunc
	resp           *http.Response
	sentences      chan string
	done           chan struct{}
	conversationID string

	// Filled by reader goroutine before close(done).
	action Action
	err    error

	closed atomic.Bool
}

func (s *httpTurnStream) Sentences() <-chan string { return s.sentences }

func (s *httpTurnStream) Action() (Action, error) {
	<-s.done
	return s.action, s.err
}

// Close releases the HTTP connection and unblocks any in-flight reader.
// Safe to call multiple times.
func (s *httpTurnStream) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancel()
	return nil
}

func (s *httpTurnStream) read() {
	defer close(s.sentences)
	defer close(s.done)
	defer s.resp.Body.Close()

	parser := &SentenceParser{}
	buf := make([]byte, 1024)

	for {
		n, err := s.resp.Body.Read(buf)
		if n > 0 {
			for _, sent := range parser.Feed(string(buf[:n])) {
				select {
				case s.sentences <- sent:
				case <-s.ctx.Done():
					s.err = s.ctx.Err()
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.err = err
				return
			}
			s.err = fmt.Errorf("read body: %w", err)
			return
		}
	}

	leftover, action := parser.Finalize()
	if leftover != "" {
		select {
		case s.sentences <- leftover:
		case <-s.ctx.Done():
			s.err = s.ctx.Err()
			return
		}
	}
	s.action = action
	slog.Debug("bot turn finalized",
		"call_uuid", s.conversationID,
		"action", string(action),
	)
}
