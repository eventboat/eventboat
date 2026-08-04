package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/stage"
	kafkago "github.com/segmentio/kafka-go"
)

func newTestSource() *Source {
	return &Source{
		Base:    basestage.Base{IDVal: "t", KindVal: stage.KindSource, TypeVal: "kafka"},
		brokers: []string{"localhost:1"},
		topics:  []string{"t"},
		groupID: "g",
		commitTimeout: 50 * time.Millisecond,
		pending: make(map[string]kafkago.Message),
	}
}

func TestStopClearsPending(t *testing.T) {
	s := newTestSource()
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.pending["msg-1"] = kafkago.Message{Topic: "t", Offset: 1}
	s.pending["msg-2"] = kafkago.Message{Topic: "t", Offset: 2}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	n := len(s.pending)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected pending cleared on Stop, got %d entries", n)
	}
}

func TestOnAckAfterStopDoesNotCommit(t *testing.T) {
	s := newTestSource()
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	msg := message.New([]byte("v"), nil)
	msg.ID = "msg-1"
	s.pending[msg.ID] = kafkago.Message{Topic: "t", Offset: 1}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Must not panic or block committing on a closed reader.
	done := make(chan struct{})
	go func() {
		s.OnAck(msg, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnAck blocked after Stop")
	}
}

// TestOnAckConcurrentWithStop exercises the reader-access race under -race.
func TestOnAckConcurrentWithStop(t *testing.T) {
	s := newTestSource()
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		m := message.New([]byte("v"), nil)
		m.ID = string(rune('a' + i))
		s.pending[m.ID] = kafkago.Message{Topic: "t", Offset: int64(i)}
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := message.New([]byte("v"), nil)
			m.ID = string(rune('a' + i))
			s.OnAck(m, nil)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Stop(context.Background())
	}()
	wg.Wait()
}

func TestDoubleStopIsSafe(t *testing.T) {
	s := newTestSource()
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}
