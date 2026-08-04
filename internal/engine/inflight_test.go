package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/topology"
)

func TestAcquireInflightBlocksAtLimit(t *testing.T) {
	p := &Pipeline{
		ir: &topology.TopologyIR{
			Name:   "p1",
			Engine: config.EngineConfig{MaxInflight: 1},
		},
	}
	if err := p.acquireInflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.InflightCount() != 1 {
		t.Fatalf("inflight = %d", p.InflightCount())
	}

	done := make(chan error, 1)
	go func() {
		done <- p.acquireInflight(context.Background())
	}()

	select {
	case err := <-done:
		t.Fatalf("expected block, got %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	p.releaseInflight(nil, time.Now())
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBeginMessageLifecycleReleasesOnAck(t *testing.T) {
	p := &Pipeline{
		ir: &topology.TopologyIR{
			Name:   "p1",
			Engine: config.EngineConfig{MaxInflight: 2},
		},
	}
	msg := message.New(nil, nil)
	if err := p.beginMessageLifecycle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	msg.Ack(nil)
	if p.InflightCount() != 0 {
		t.Fatalf("inflight = %d after ack", p.InflightCount())
	}
}

func TestAckMessageErrorIgnoreMode(t *testing.T) {
	var acks atomic.Int32
	p := &Pipeline{
		ir: &topology.TopologyIR{
			Name:   "p1",
			Engine: config.EngineConfig{ErrorMode: "propagate"},
		},
		stageErrorMode: map[string]string{"map": "ignore"},
	}
	msg := message.New(nil, nil)
	msg.SetAckFn(func(err error) {
		if err != nil {
			t.Fatalf("unexpected ack error: %v", err)
		}
		acks.Add(1)
	})
	p.ackMessageError("map", msg, context.Canceled)
	if acks.Load() != 1 {
		t.Fatalf("acks = %d", acks.Load())
	}
}

func TestAckMessageErrorPropagateMode(t *testing.T) {
	var got error
	p := &Pipeline{
		ir: &topology.TopologyIR{
			Name:   "p1",
			Engine: config.EngineConfig{ErrorMode: "propagate"},
		},
	}
	msg := message.New(nil, nil)
	msg.SetAckFn(func(err error) {
		got = err
	})
	want := context.Canceled
	p.ackMessageError("map", msg, want)
	if got != want {
		t.Fatalf("got %v want %v", got, want)
	}
}
