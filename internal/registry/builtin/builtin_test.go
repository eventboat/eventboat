// Package builtin tests exercise schema validation and factory error paths
// directly, without any real broker or network dependency (M1 debt: the
// builtins were previously covered only through pipeline-level tests).
package builtin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/eventboat/eventboat/internal/registry"
)

func newReg(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

// --- codecs ---

func TestCodecsRoundTrip(t *testing.T) {
	reg := newReg(t)
	jc, err := reg.NewCodec("json", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	v, err := jc.Decode([]byte(`{"a":1,"b":["x"]}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := jc.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"a":1,"b":["x"]}` {
		t.Errorf("json round trip = %s", out)
	}
	if _, err := jc.Decode([]byte("")); err == nil {
		t.Error("empty payload accepted")
	}
	if _, err := jc.Decode([]byte("{not json")); err == nil {
		t.Error("malformed json accepted")
	}

	rc, err := reg.NewCodec("raw", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := rc.Decode([]byte("plain")); err != nil || v != "plain" {
		t.Errorf("raw decode = %v, %v", v, err)
	}
	if _, err := rc.Encode(42); err == nil {
		t.Error("raw encode of non-string accepted")
	}
	if _, err := reg.NewCodec("nope", nil, ""); err == nil {
		t.Error("unknown codec accepted")
	}
}

// --- cron source ---

func TestCronSourceFactoryPaths(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSource("cron", map[string]any{}); err == nil {
		t.Error("cron without expression accepted")
	}
	if _, err := reg.NewSource("cron", map[string]any{"expression": 5}); err == nil {
		t.Error("cron with non-string expression accepted")
	}
	if _, err := reg.NewSource("cron", map[string]any{"expression": "not a cron", "extra": 1}); err == nil {
		t.Error("cron with invalid expression accepted")
	}
	src, err := reg.NewSource("cron", map[string]any{"expression": "0 1 * * *", "payload": `{"tick":true}`})
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Error(err)
	}
}

// --- file source ---

func TestFileSourceSchemaAndTail(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSource("file", map[string]any{"path": ""}); err == nil {
		t.Error("file source without path accepted")
	}
	if _, err := reg.NewSource("file", map[string]any{"path": "a", "bogus": 1}); err == nil {
		t.Error("file source with unknown field accepted")
	}

	path := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(path, []byte("{\"i\":1}\n\n{\"i\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := reg.NewSource("file", map[string]any{"path": path, "poll_every_ms": 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan registry.Message, 8)
	go src.Run(ctx, func(m registry.Message) { got <- m })
	deadline := time.After(2 * time.Second)
	n := 0
loop:
	for {
		select {
		case m := <-got:
			n++
			if m.SrcSeq <= 0 {
				t.Errorf("emission without srcSeq: %+v", m)
			}
			if n == 2 {
				break loop
			}
		case <-deadline:
			t.Fatalf("tailed %d messages, want 2 (blank lines skipped)", n)
		}
	}
	cancel()
	_ = src.Close()

	// Commit persists the byte offset of the highest committed emission; Init
	// restores it, so a reopened source resumes after committed lines.
	state, err := src.Commit(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) == 0 {
		t.Fatal("file source returned no commit state")
	}
	src2, err := reg.NewSource("file", map[string]any{"path": path, "poll_every_ms": 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := src2.Init(state); err != nil {
		t.Fatal(err)
	}
}

// --- file sink + drop sink ---

func TestFileSinkWritesAndSchema(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSink("file", map[string]any{}); err == nil {
		t.Error("file sink without path accepted")
	}
	path := filepath.Join(t.TempDir(), "nested", "out.jsonl")
	sink, err := reg.NewSink("file", map[string]any{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), []registry.Message{{Out: []byte(`{"a":1}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.HasSuffix(string(data), "\n") {
		t.Errorf("file sink output = %q, %v", data, err)
	}

	drop, err := reg.NewSink("drop", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := drop.Write(context.Background(), []registry.Message{{Out: []byte("x")}}); err != nil {
		t.Error(err)
	}
	if _, err := reg.NewSink("drop", map[string]any{"nope": 1}); err == nil {
		t.Error("drop sink with unknown field accepted")
	}
}

// --- http sink (httptest backend; no real network) ---

func TestHTTPSinkPostsJSON(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSink("http", map[string]any{}); err == nil {
		t.Error("http sink without url accepted")
	}
	var body string
	var ct string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		body, ct = string(b), r.Header.Get("Content-Type")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sink, err := reg.NewSink("http", map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), []registry.Message{{Out: []byte(`{"m":1}`)}}); err != nil {
		t.Fatal(err)
	}
	if body == "" {
		t.Fatal("http sink posted nothing")
	}
	if !strings.Contains(ct, "json") {
		t.Errorf("content type = %q", ct)
	}
	_ = sink.Close()
}

// --- http_server source: schema only (Run needs a listener; covered by the
// engine testkit elsewhere) ---

func TestHTTPServerSourceSchema(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSource("http_server", map[string]any{}); err == nil {
		t.Error("http_server without listen accepted")
	}
	if _, err := reg.NewSource("http_server", map[string]any{"listen": "127.0.0.1:0", "method": "GET"}); err == nil {
		t.Error("http_server with unknown field accepted")
	}
	if _, err := reg.NewSource("http_server", map[string]any{"listen": "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
}

// --- kafka source/sink: schema gates without any broker (kafka-go connects
// lazily, so valid configs construct offline) ---

func TestKafkaSchemaGatesWithoutBroker(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSource("kafka", map[string]any{"brokers": []any{"localhost:9092"}}); err == nil {
		t.Error("kafka source without topics accepted")
	}
	if _, err := reg.NewSource("kafka", map[string]any{"brokers": []any{}, "topics": []any{"t"}}); err == nil {
		t.Error("kafka source with empty brokers accepted")
	}
	if _, err := reg.NewSource("kafka", map[string]any{
		"brokers": []any{"localhost:9092"}, "topics": []any{"orders"}, "group_id": "g",
	}); err != nil {
		t.Fatalf("valid kafka source config rejected offline: %v", err)
	}

	if _, err := reg.NewSink("kafka", map[string]any{"brokers": []any{"localhost:9092"}}); err == nil {
		t.Error("kafka sink without topic accepted")
	}
	if _, err := reg.NewSink("kafka", map[string]any{
		"brokers": []any{"localhost:9092"}, "topic": "out",
	}); err != nil {
		t.Fatalf("valid kafka sink config rejected offline: %v", err)
	}
}

// --- kafka/file source Commit scans: the engine calls Commit on every
// frontier advance (~per message), so the scan starts at the per-source
// watermark of already-folded seqs, never at 1 — a full rescan per call made
// total Commit cost O(N²) and throughput decayed over runtime. Without a
// reader attached, kafka Commit still drains pending and advances the
// watermark (the broker flush is the only reader-dependent step). ---

func TestKafkaSourceCommitScanBoundedByWatermark(t *testing.T) {
	s := &kafkaSource{pending: map[int64]kafka.Message{}}
	for seq := int64(1); seq <= 200; seq++ {
		s.pending[seq] = kafka.Message{Offset: seq}
	}
	ctx := context.Background()
	for seq := int64(1); seq <= 200; seq++ {
		if _, err := s.Commit(ctx, seq); err != nil {
			t.Fatalf("commit through %d: %v", seq, err)
		}
	}
	if len(s.pending) != 0 {
		t.Fatalf("pending holds %d entries after a full drain, want 0", len(s.pending))
	}
	if s.lastCommitted != 200 {
		t.Fatalf("lastCommitted = %d, want 200", s.lastCommitted)
	}
	// A regressed frontier must not walk the watermark back.
	if _, err := s.Commit(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if s.lastCommitted != 200 {
		t.Fatalf("lastCommitted = %d after a regressed call, want 200", s.lastCommitted)
	}
	// Later emissions are scanned from the watermark onward.
	s.pending[201] = kafka.Message{Offset: 201}
	if _, err := s.Commit(ctx, 201); err != nil {
		t.Fatal(err)
	}
	if len(s.pending) != 0 || s.lastCommitted != 201 {
		t.Fatalf("pending=%d lastCommitted=%d, want 0/201", len(s.pending), s.lastCommitted)
	}
}

func TestFileSourceCommitScanBoundedByWatermark(t *testing.T) {
	reg := newReg(t)
	src, err := reg.NewSource("file", map[string]any{"path": filepath.Join(t.TempDir(), "in.jsonl")})
	if err != nil {
		t.Fatal(err)
	}
	fs := src.(*fileSource)
	fs.pending = map[int64]int64{1: 10, 2: 25, 3: 40}
	ctx := context.Background()
	var state []byte
	for seq := int64(1); seq <= 3; seq++ {
		if state, err = fs.Commit(ctx, seq); err != nil {
			t.Fatalf("commit through %d: %v", seq, err)
		}
	}
	if !strings.Contains(string(state), `"offset":40`) {
		t.Fatalf("commit state = %s, want offset 40", state)
	}
	if len(fs.pending) != 0 {
		t.Fatalf("pending holds %d entries after a full drain, want 0", len(fs.pending))
	}
	if fs.lastCommitted != 3 {
		t.Fatalf("lastCommitted = %d, want 3", fs.lastCommitted)
	}
	// A regressed frontier neither rescans nor regresses the offset.
	if state, err = fs.Commit(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"offset":40`) {
		t.Fatalf("commit state = %s after a regressed call, want offset 40 still", state)
	}
}
