package httpserver

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/stage"
)

func newTestSource(addr string, maxBody int64) *Source {
	return &Source{
		Base:         basestage.Base{IDVal: "t", KindVal: stage.KindSource, TypeVal: "http_server"},
		addr:         addr,
		path:         "/",
		maxBodyBytes: maxBody,
	}
}

func TestInitFailsWhenPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	s := newTestSource(ln.Addr().String(), defaultMaxBodyBytes)
	if err := s.Init(context.Background()); err == nil {
		t.Fatal("expected Init error when port is occupied")
	}
}

func TestInitSucceedsOnFreePort(t *testing.T) {
	s := newTestSource("127.0.0.1:0", defaultMaxBodyBytes)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Stop(context.Background()) }()
}

func TestHandleRejectsOversizedBody(t *testing.T) {
	s := newTestSource("", 16)
	s.out = make(chan *message.Message, 1)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(make([]byte, 1024)))
	s.handle(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestHandleAcceptsBodyWithinLimit(t *testing.T) {
	s := newTestSource("", defaultMaxBodyBytes)
	out := make(chan *message.Message, 1)
	s.out = out

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"ok":true}`)))
	s.handle(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	select {
	case m := <-out:
		if string(m.Payload) != `{"ok":true}` {
			t.Fatalf("payload = %q", m.Payload)
		}
	default:
		t.Fatal("expected message on out channel")
	}
}

func TestConsumeReportsServeFailure(t *testing.T) {
	s := newTestSource("127.0.0.1:0", defaultMaxBodyBytes)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Force a serve failure after Init by closing the listener directly.
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln == nil {
		t.Fatal("expected listener after Init")
	}
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Consume(ctx, make(chan *message.Message)); err == nil {
		t.Fatal("expected Consume to report serve failure")
	}
}
