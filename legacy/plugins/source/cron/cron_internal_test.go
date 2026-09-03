package cron

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/stage"
)

func newTestSource(body string) *Source {
	return &Source{
		Base:     basestage.Base{IDVal: "t", KindVal: stage.KindSource, TypeVal: "cron"},
		schedule: "* * * * * *",
		body:     body,
	}
}

// A tick must not be dropped silently under backpressure: emit blocks until
// the consumer reads or the context is cancelled.
func TestEmitBlocksOnBackpressure(t *testing.T) {
	s := newTestSource("")
	s.out = make(chan *message.Message) // unbuffered, nobody reads

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.emit(ctx)
	if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
		t.Fatalf("emit returned after %v; tick was dropped instead of blocked", elapsed)
	}
}

func TestEmitDeliversWhenConsumerReads(t *testing.T) {
	s := newTestSource(`{"price":10}`)
	out := make(chan *message.Message, 1)
	s.out = out

	s.emit(context.Background())
	select {
	case m := <-out:
		if m.ParsedData() == nil {
			t.Fatal("expected parsed data for valid json payload")
		}
	default:
		t.Fatal("expected message on out channel")
	}
}

// Malformed payload must not fail silently: it is still emitted (parsed data
// skipped) but a warning is logged.
func TestEmitLogsMalformedPayload(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	s := newTestSource(`not json`)
	out := make(chan *message.Message, 1)
	s.out = out

	s.emit(context.Background())
	select {
	case m := <-out:
		if string(m.Payload) != "not json" {
			t.Fatalf("payload = %q", m.Payload)
		}
		if m.ParsedData() != nil {
			t.Fatal("expected no parsed data for malformed payload")
		}
	default:
		t.Fatal("expected message on out channel")
	}
	if buf.Len() == 0 {
		t.Fatal("expected a log warning for malformed payload")
	}
}
