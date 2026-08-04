package httpsink_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	_ "github.com/edgesets/edgestream/plugins/sink/http"
)

func newSink(t *testing.T, cfg map[string]any) stage.Sink {
	t.Helper()
	sk, err := registry.Default.CreateSink("http", "t", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return sk
}

func TestHTTPSink_RequiresURL(t *testing.T) {
	_, err := registry.Default.CreateSink("http", "t", map[string]any{})
	if err == nil {
		t.Fatal("expected error when url missing")
	}
}

func TestHTTPSink_RespectsConfiguredTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	sk := newSink(t, map[string]any{"url": srv.URL, "timeout": "50ms"})
	start := time.Now()
	err := sk.Write(context.Background(), []*message.Message{message.New([]byte(`{}`), nil)})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Fatalf("write took too long, timeout config not applied: %v", time.Since(start))
	}
}

func TestHTTPSink_InvalidTimeout(t *testing.T) {
	_, err := registry.Default.CreateSink("http", "t", map[string]any{
		"url":     "http://example.com",
		"timeout": "not-a-duration",
	})
	if err == nil {
		t.Fatal("expected error for invalid timeout")
	}
}

func TestHTTPSink_BatchContinuesAfterSingleFailure(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sk := newSink(t, map[string]any{"url": srv.URL})
	msgs := []*message.Message{
		message.New([]byte(`{"n":1}`), nil),
		message.New([]byte(`{"n":2}`), nil),
		message.New([]byte(`{"n":3}`), nil),
	}
	err := sk.Write(context.Background(), msgs)
	if err == nil {
		t.Fatal("expected error from failed message")
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("expected all 3 messages attempted, got %d requests", got)
	}
}

func TestHTTPSink_ContentTypeFromCodec(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
	}))
	defer srv.Close()

	sk := newSink(t, map[string]any{"url": srv.URL})

	rawMsg := message.New([]byte("hello"), nil)
	rawMsg.SetParsedCodec("raw")
	if err := sk.Write(context.Background(), []*message.Message{rawMsg}); err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/octet-stream" {
		t.Fatalf("raw payload Content-Type = %q", gotCT)
	}

	jsonMsg := message.New([]byte(`{"a":1}`), nil)
	jsonMsg.SetParsedCodec("json")
	if err := sk.Write(context.Background(), []*message.Message{jsonMsg}); err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/json" {
		t.Fatalf("json payload Content-Type = %q", gotCT)
	}

	plain := message.New([]byte(`{"a":1}`), nil)
	if err := sk.Write(context.Background(), []*message.Message{plain}); err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/json" {
		t.Fatalf("default Content-Type = %q", gotCT)
	}
}

func TestHTTPSink_AggregatesErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	sk := newSink(t, map[string]any{"url": srv.URL})
	err := sk.Write(context.Background(), []*message.Message{
		message.New([]byte(`{"n":1}`), nil),
		message.New([]byte(`{"n":2}`), nil),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "2/2") {
		t.Fatalf("expected failure count in error, got %v", err)
	}
}
