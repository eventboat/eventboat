// Command ticker-source is a third-party-style Eventboat source plugin: a
// price ticker that emits a configurable number of events per pull. It is a
// separate Go module and depends only on the generated protocol code in
// pkg/pluginv1 — exactly what an outside implementer would use, per
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

	pluginv1 "github.com/eventboat/eventboat/pkg/pluginv1"
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

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(authUnary(token)), grpc.ChainStreamInterceptor(authStream(token)))
	pluginv1.RegisterSourceServer(srv, src)
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
}

// tickerSource emits Events events per pull, one every IntervalMs, resuming
// from the persisted state (the last settled sequence number).
type tickerSource struct {
	pluginv1.UnimplementedSourceServer
	mu       sync.Mutex
	cfg      config
	state    int64 // last settled sequence (from Init)
	lastSent int64 // last sequence emitted this session
}

func (t *tickerSource) Init(ctx context.Context, req *pluginv1.InitRequest) (*pluginv1.InitResponse, error) {
	fmt.Fprintf(os.Stderr, "ticker-source: init config=%q state=%q\n", req.ConfigJson, req.State)
	if req.ConfigJson != "" {
		var c config
		if err := json.Unmarshal([]byte(req.ConfigJson), &c); err != nil {
			return &pluginv1.InitResponse{Error: "bad config json: " + err.Error()}, nil
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
	}
	if len(req.State) > 0 {
		var s struct {
			Last int64 `json:"last"`
		}
		if err := json.Unmarshal(req.State, &s); err != nil {
			return &pluginv1.InitResponse{Error: "bad state: " + err.Error()}, nil
		}
		t.state = s.Last
		t.lastSent = s.Last
	}
	return &pluginv1.InitResponse{}, nil
}

// Run is the continuous mode: keep ticking until the host cancels.
func (t *tickerSource) Run(req *pluginv1.RunRequest, stream pluginv1.Source_RunServer) error {
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
// The end bound is fixed up front: Settled RPCs advance t.state concurrently
// with this stream, and re-reading the bound each iteration would turn the
// bounded pull into an endless generator.
func (t *tickerSource) Pull(req *pluginv1.RunRequest, stream pluginv1.Source_PullServer) error {
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

func (t *tickerSource) Settled(ctx context.Context, req *pluginv1.SettledRequest) (*pluginv1.SettledResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if req.ThroughSrcSeq > 0 && req.ThroughSrcSeq <= t.lastSent {
		t.state = req.ThroughSrcSeq
	}
	state, _ := json.Marshal(map[string]int64{"last": t.state})
	return &pluginv1.SettledResponse{State: state}, nil
}

func (t *tickerSource) Close(ctx context.Context, req *pluginv1.CloseRequest) (*pluginv1.CloseResponse, error) {
	return &pluginv1.CloseResponse{}, nil
}

// event builds one wire Event. The sequence is deterministic: the price is a
// stable function of the tick number.
func (t *tickerSource) event(seq int64) *pluginv1.Event {
	payload, _ := json.Marshal(map[string]any{
		"symbol": t.cfg.Symbol,
		"seq":    seq,
		"price":  100 + float64((seq*37)%1000)/10.0,
		"time":   time.Now().UTC().Format(time.RFC3339Nano),
	})
	return &pluginv1.Event{
		Payload: payload,
		Meta: map[string]*pluginv1.MetaValue{
			"symbol": {Kind: &pluginv1.MetaValue_StringValue{StringValue: t.cfg.Symbol}},
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
