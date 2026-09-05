// Command ticker-source is a third-party-style Eventboat source plugin: a
// price ticker that emits a configurable number of events per pull. It is a
// separate Go module and depends only on the generated protocol code in
// pkg/pluginproto — exactly what an outside implementer would use, per
// docs/plugins.md.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/eventboat/eventboat/pkg/pluginproto"
)

func main() {
	cfg := config{Symbol: "USD/EUR", Events: 10, IntervalMs: 100}
	src := &tickerSource{cfg: cfg}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ticker-source: listen: %v\n", err)
		os.Exit(1)
	}
	token, err := randomToken()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ticker-source: token: %v\n", err)
		os.Exit(1)
	}

	// The handshake line: everything the host needs to dial us.
	handshake, _ := json.Marshal(map[string]any{
		"eventboat_plugin": 1,
		"kind":             "source",
		"name":             "ticker",
		"version":          1,
		"capabilities":     []string{"pull"},
		"listen":           lis.Addr().String(),
		"auth":             token,
	})
	fmt.Println(string(handshake))

	// MaxRecv/MaxSend mirror the host's transport contract: 64 MiB messages
	// both ways (docs/plugins.md) — above grpc-go's 4 MiB default so legal
	// large Events pass, still bounded.
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
		grpc.ChainUnaryInterceptor(authUnary(token)),
		grpc.ChainStreamInterceptor(authStream(token)))
	pluginproto.RegisterSourceServer(srv, src)
	healthpb.RegisterHealthServer(srv, health.NewServer())

	// Stop convention: the host closes our stdin to shut us down.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "ticker-source: serve: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	Symbol     string `json:"symbol"`
	Events     int    `json:"events"`
	IntervalMs int    `json:"interval_ms"`
	PadBytes   int    `json:"pad_bytes"` // test hook: pad each payload (big-message transport tests)
}

// tickerSource emits Events events per pull, one every IntervalMs, resuming
// from the persisted state (the last committed sequence number).
type tickerSource struct {
	pluginproto.UnimplementedSourceServer
	mu       sync.Mutex
	cfg      config
	state    int64 // last committed sequence (from Init)
	lastSent int64 // last sequence emitted this session
}

func (t *tickerSource) Init(ctx context.Context, req *pluginproto.InitRequest) (*pluginproto.InitResponse, error) {
	fmt.Fprintf(os.Stderr, "ticker-source: init config=%q state=%q\n", req.ConfigJson, req.State)
	if req.ConfigJson != "" {
		var c config
		if err := json.Unmarshal([]byte(req.ConfigJson), &c); err != nil {
			return &pluginproto.InitResponse{Error: "bad config json: " + err.Error()}, nil
		}
		if c.Symbol != "" {
			t.cfg.Symbol = c.Symbol
		}
		if c.Events > 0 {
			t.cfg.Events = c.Events
		}
		if c.IntervalMs > 0 {
			t.cfg.IntervalMs = c.IntervalMs
		}
		if c.PadBytes > 0 {
			t.cfg.PadBytes = c.PadBytes
		}
	}
	if len(req.State) > 0 {
		var s struct {
			Last int64 `json:"last"`
		}
		if err := json.Unmarshal(req.State, &s); err != nil {
			return &pluginproto.InitResponse{Error: "bad state: " + err.Error()}, nil
		}
		t.state = s.Last
		t.lastSent = s.Last
	}
	return &pluginproto.InitResponse{}, nil
}

// Run is the continuous mode: keep ticking until the host cancels.
func (t *tickerSource) Run(req *pluginproto.RunRequest, stream pluginproto.Source_RunServer) error {
	fmt.Fprintf(os.Stderr, "ticker-source: RUN (continuous)\n")
	for i := t.lastSent + 1; ; i++ {
		if err := stream.Context().Err(); err != nil {
			return nil // host stopped us: normal shutdown
		}
		if err := stream.Send(t.event(i)); err != nil {
			return err
		}
		t.mu.Lock()
		t.lastSent = i
		t.mu.Unlock()
		sleepMs(stream.Context(), t.cfg.IntervalMs)
	}
}

// Pull is the job mode: emit cfg.Events events after the persisted state,
// then end the stream with OK status — the documented "exhausted" signal.
// The end bound is fixed up front: Commit RPCs advance t.state concurrently
// with this stream, and re-reading the bound each iteration would turn the
// bounded pull into an endless generator.
func (t *tickerSource) Pull(req *pluginproto.RunRequest, stream pluginproto.Source_PullServer) error {
	fmt.Fprintf(os.Stderr, "ticker-source: PULL events=%d\n", t.cfg.Events)
	start := t.state
	end := start + int64(t.cfg.Events)
	for i := start + 1; i <= end; i++ {
		if err := stream.Context().Err(); err != nil {
			return status.Error(codes.Canceled, "canceled")
		}
		if err := stream.Send(t.event(i)); err != nil {
			return err
		}
		t.mu.Lock()
		t.lastSent = i
		t.mu.Unlock()
		sleepMs(stream.Context(), t.cfg.IntervalMs)
	}
	return nil // exhausted
}

func (t *tickerSource) Commit(ctx context.Context, req *pluginproto.CommitRequest) (*pluginproto.CommitResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if req.ThroughSrcSeq > 0 && req.ThroughSrcSeq <= t.lastSent {
		t.state = req.ThroughSrcSeq
	}
	state, _ := json.Marshal(map[string]int64{"last": t.state})
	return &pluginproto.CommitResponse{State: state}, nil
}

func (t *tickerSource) Close(ctx context.Context, req *pluginproto.CloseRequest) (*pluginproto.CloseResponse, error) {
	return &pluginproto.CloseResponse{}, nil
}

// event builds one wire Event. The sequence is deterministic: the price is a
// stable function of the tick number. pad_bytes (when configured) appends a
// filler field so the payload crosses a chosen size — used by transport
// tests to exercise large messages.
func (t *tickerSource) event(seq int64) *pluginproto.Event {
	payload, _ := json.Marshal(map[string]any{
		"symbol": t.cfg.Symbol,
		"seq":    seq,
		"price":  100 + float64((seq*37)%1000)/10.0,
		"time":   time.Now().UTC().Format(time.RFC3339Nano),
	})
	if t.cfg.PadBytes > len(payload) {
		padding := make([]byte, t.cfg.PadBytes-len(payload)-len(`,"pad":""`))
		padded := append(payload[:len(payload)-1], []byte(`,"pad":"`+string(padding)+`"}`)...)
		payload = padded
	}
	return &pluginproto.Event{
		Payload: payload,
		Meta: map[string]*pluginproto.MetaValue{
			"symbol": {Kind: &pluginproto.MetaValue_StringValue{StringValue: t.cfg.Symbol}},
		},
		Codec:   "json",
		Cursor:  strconv.FormatInt(seq, 10),
		SrcSeq:  seq,
		SrcName: "ticker",
	}
}

func sleepMs(ctx context.Context, ms int) {
	if ms <= 0 {
		ms = 1
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(ms) * time.Millisecond):
	}
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func authUnary(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !authorized(ctx, token) {
			return nil, status.Error(codes.Unauthenticated, "missing or wrong eventboat-auth metadata")
		}
		return handler(ctx, req)
	}
}

func authStream(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !authorized(ss.Context(), token) {
			return status.Error(codes.Unauthenticated, "missing or wrong eventboat-auth metadata")
		}
		return handler(srv, ss)
	}
}

func authorized(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	got := md.Get("eventboat-auth")
	return len(got) == 1 && got[0] == token
}
