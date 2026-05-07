package asr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "callbot-master/proto/streaming_voice"
)

// ViettelClient implements asr.Client against the Viettel STT gRPC bidi
// streaming endpoint (proto/streaming_voice). One *ClientConn is shared by
// all in-flight streams.
type ViettelClient struct {
	addr  string
	token string
	conn  *grpc.ClientConn
	api   pb.StreamVoiceClient
}

// NewViettelClient lazily dials addr; returns an error if dial fails.
// Use Close on shutdown.
func NewViettelClient(ctx context.Context, addr, token string) (*ViettelClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("asr dial: %w", err)
	}
	return &ViettelClient{
		addr:  addr,
		token: token,
		conn:  conn,
		api:   pb.NewStreamVoiceClient(conn),
	}, nil
}

// Close releases the gRPC client connection.
func (c *ViettelClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *ViettelClient) StartStream(ctx context.Context, opts StreamOpts) (Stream, error) {
	rate := opts.SampleRate
	if rate == 0 {
		rate = 8000
	}
	channels := opts.Channels
	if channels == 0 {
		channels = 1
	}
	md := metadata.New(map[string]string{
		"channels":        strconv.Itoa(channels),
		"rate":            strconv.Itoa(rate),
		"format":          "S16LE",
		"token":           c.token,
		"id":              opts.ConversationID,
		"single-sentence": boolStr(opts.SingleSentence),
		"silence_timeout": toSecStr(opts.SilenceTimeoutMs),
		"speech_timeout":  toSecStr(opts.SpeechTimeoutMs),
		"speech_max":      toSecStr(opts.SpeechMaxMs),
	})
	streamCtx, cancel := context.WithCancel(ctx)
	streamCtx = metadata.NewOutgoingContext(streamCtx, md)

	gs, err := c.api.SendVoice(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("asr open stream: %w", err)
	}

	s := &viettelStream{
		conversationID: opts.ConversationID,
		grpc:           gs,
		out:            make(chan Result, 8),
		cancel:         cancel,
		ctx:            streamCtx,
	}
	go s.recvLoop()
	return s, nil
}

type viettelStream struct {
	conversationID string
	grpc           pb.StreamVoice_SendVoiceClient
	out            chan Result

	ctx    context.Context
	cancel context.CancelFunc

	sendMu     sync.Mutex
	sendClosed atomic.Bool
	closed     atomic.Bool
}

func (s *viettelStream) SendAudio(pcm []byte) error {
	if s.sendClosed.Load() || s.closed.Load() {
		return errors.New("asr stream send closed")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.grpc.Send(&pb.VoiceRequest{ByteBuff: pcm})
}

func (s *viettelStream) Recv() <-chan Result { return s.out }

// Close cancels the stream context and stops the recv loop. Safe to call
// multiple times. CloseSend is called as a courtesy so the server flushes
// any buffered final transcript before EOF.
func (s *viettelStream) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.sendMu.Lock()
	if !s.sendClosed.Swap(true) {
		_ = s.grpc.CloseSend()
	}
	s.sendMu.Unlock()
	s.cancel()
	return nil
}

// CloseSend ends the upload side without aborting receive. Useful when the
// VAD signals end-of-utterance and we want the server to emit a final
// transcript.
func (s *viettelStream) CloseSend() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendClosed.Swap(true) {
		return nil
	}
	return s.grpc.CloseSend()
}

func (s *viettelStream) recvLoop() {
	defer close(s.out)
	for {
		reply, err := s.grpc.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) && !s.closed.Load() {
				slog.Debug("asr recv ended", "call_uuid", s.conversationID, "err", err)
			}
			return
		}
		if reply.GetStatus() != 0 {
			slog.Warn("asr server status",
				"call_uuid", s.conversationID,
				"status", reply.GetStatus(),
				"msg", reply.GetMsg())
			continue
		}
		res := reply.GetResult()
		if res == nil || len(res.GetHypotheses()) == 0 {
			continue
		}
		h := res.GetHypotheses()[0]
		// Prefer raw transcript over normed: the newer relay client
		// (callbot-realtime-agents-main/src/app/lib/asrClient.ts) uses
		// hypothesis.transcript and our user reports the v2 path "feels
		// slower" than that one. The normed field is a post-processing
		// pass on top of transcript and arrives populated in the same
		// gRPC message, but in practice we've seen the raw field be
		// surfaced earlier in partial frames. Falling back keeps us
		// working if the server only fills normed.
		text := h.GetTranscript()
		if text == "" {
			text = h.GetTranscriptNormed()
		}
		select {
		case s.out <- Result{Text: text, IsFinal: res.GetFinal()}:
		case <-s.ctx.Done():
			return
		}
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// Viettel ASR's metadata fields expect *integer seconds* as a string —
// "1" / "5" / "30", never "0.8" or "1s". Sub-second values are rounded UP
// to 1 so a config of e.g. 800ms doesn't silently become 0.
func toSecStr(ms int) string {
	if ms <= 0 {
		return "0"
	}
	s := ms / 1000
	if s == 0 {
		s = 1
	}
	return strconv.Itoa(s)
}
