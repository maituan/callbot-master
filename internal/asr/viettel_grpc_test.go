package asr

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "callbot-master/proto/streaming_voice"
)

func TestToSecStr(t *testing.T) {
	cases := []struct {
		ms   int
		want string
	}{
		{0, "0"},
		{-1, "0"},
		{500, "0.5"},
		{800, "0.8"},
		{1000, "1"},
		{1500, "1.5"},
		{1200, "1.2"},
		{30000, "30"},
		{12750, "12.75"},
	}
	for _, c := range cases {
		got := toSecStr(c.ms)
		if got != c.want {
			t.Errorf("toSecStr(%d) = %q, want %q", c.ms, got, c.want)
		}
	}
}

// fakeServer implements StreamVoiceServer with a caller-provided handler.
type fakeServer struct {
	pb.UnimplementedStreamVoiceServer
	handler func(stream pb.StreamVoice_SendVoiceServer) error
}

func (f *fakeServer) SendVoice(stream pb.StreamVoice_SendVoiceServer) error {
	return f.handler(stream)
}

// startBufconnServer spins up an in-process gRPC server backed by bufconn,
// returning a *grpc.ClientConn dialed against it. Cleanup is registered with t.
func startBufconnServer(t *testing.T, handler func(pb.StreamVoice_SendVoiceServer) error) *ViettelClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterStreamVoiceServer(srv, &fakeServer{handler: handler})
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &ViettelClient{
		addr:  "bufconn",
		token: "test-token",
		conn:  conn,
		api:   pb.NewStreamVoiceClient(conn),
	}
}

func TestViettelASR_StreamsAudioAndReceivesFinal(t *testing.T) {
	gotFrames := make(chan int, 4)

	c := startBufconnServer(t, func(stream pb.StreamVoice_SendVoiceServer) error {
		// Drain incoming frames, send a partial then a final.
		for {
			req, err := stream.Recv()
			if err != nil {
				break
			}
			gotFrames <- len(req.GetByteBuff())
		}
		_ = stream.Send(&pb.TextReply{
			Status: 0,
			Result: &pb.Result{
				Hypotheses: []*pb.Result_Hypothese{{TranscriptNormed: "xin chào"}},
				Final:      false,
			},
		})
		_ = stream.Send(&pb.TextReply{
			Status: 0,
			Result: &pb.Result{
				Hypotheses: []*pb.Result_Hypothese{{TranscriptNormed: "xin chào em là callbot"}},
				Final:      true,
			},
		})
		return nil
	})

	stream, err := c.StartStream(context.Background(), StreamOpts{
		ConversationID:   "asr-test-1",
		SampleRate:       8000,
		Channels:         1,
		SingleSentence:   true,
		SilenceTimeoutMs: 800,
		SpeechMaxMs:      30000,
	})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}
	defer stream.Close()

	// Send a few PCM-shaped frames.
	frame := make([]byte, 640) // 20ms @ 8kHz S16LE
	for i := 0; i < 3; i++ {
		if err := stream.SendAudio(frame); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	// Tell server we're done sending.
	if v, ok := stream.(*viettelStream); ok {
		if err := v.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
	}

	var results []Result
	deadline := time.After(2 * time.Second)
	for {
		select {
		case r, ok := <-stream.Recv():
			if !ok {
				goto done
			}
			results = append(results, r)
			if r.IsFinal {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for final transcript")
		}
	}
done:
	if len(results) < 2 {
		t.Fatalf("got %d results, want >=2", len(results))
	}
	final := results[len(results)-1]
	if !final.IsFinal || final.Text != "xin chào em là callbot" {
		t.Fatalf("final = %+v", final)
	}
	if got := len(gotFrames); got < 3 {
		t.Fatalf("server received %d frames, want >=3", got)
	}
}

func TestViettelASR_NonZeroStatusIsSkipped(t *testing.T) {
	c := startBufconnServer(t, func(stream pb.StreamVoice_SendVoiceServer) error {
		// Drain in.
		go func() {
			for {
				if _, err := stream.Recv(); err != nil {
					return
				}
			}
		}()
		// Emit error frame, then a clean final.
		_ = stream.Send(&pb.TextReply{Status: 1, Msg: "transient"})
		_ = stream.Send(&pb.TextReply{
			Status: 0,
			Result: &pb.Result{
				Hypotheses: []*pb.Result_Hypothese{{TranscriptNormed: "ok"}},
				Final:      true,
			},
		})
		return nil
	})

	stream, err := c.StartStream(context.Background(), StreamOpts{ConversationID: "x"})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}
	defer stream.Close()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case r, ok := <-stream.Recv():
			if !ok {
				t.Fatal("recv closed before final")
			}
			if r.IsFinal {
				if r.Text != "ok" {
					t.Fatalf("final = %q", r.Text)
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out")
		}
	}
}

func TestViettelASR_CloseStopsRecv(t *testing.T) {
	c := startBufconnServer(t, func(stream pb.StreamVoice_SendVoiceServer) error {
		// Server hangs on recv until client closes.
		for {
			if _, err := stream.Recv(); err != nil {
				return nil
			}
		}
	})

	stream, err := c.StartStream(context.Background(), StreamOpts{ConversationID: "x"})
	if err != nil {
		t.Fatalf("StartStream: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, ok := <-stream.Recv():
		if ok {
			t.Fatal("expected closed recv channel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recv channel did not close after Close()")
	}
}
