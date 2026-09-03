package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/config"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
	"github.com/riverpod/riverpod/internal/stage"
	"github.com/riverpod/riverpod/internal/testutil"
	"github.com/riverpod/riverpod/internal/topology"
)

// ackRecordSource is an AckingSource that records every OnAck result.
type ackRecordSource struct {
	basestage.Base
	payloads [][]byte
	mu       sync.Mutex
	acks     []error
}

func (s *ackRecordSource) Consume(ctx context.Context, out chan<- *message.Message) error {
	for _, p := range s.payloads {
		select {
		case out <- message.New(p, nil):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *ackRecordSource) OnAck(_ *message.Message, err error) {
	s.mu.Lock()
	s.acks = append(s.acks, err)
	s.mu.Unlock()
}

func (s *ackRecordSource) ackCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.acks)
}

func (s *ackRecordSource) ackSnapshot() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.acks...)
}

func waitForAcks(t *testing.T, src *ackRecordSource, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if src.ackCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("source got %d OnAck callbacks, want %d", src.ackCount(), want)
}

// A message that crosses a disk edge must still ack back to the AckingSource:
// the WAL roundtrip must not sever the fan-out ack chain (kafka-style sources
// commit offsets from OnAck).
func TestDiskEdgePreservesSourceAckChain(t *testing.T) {
	src := &ackRecordSource{
		Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "ack_test_source"},
		payloads: [][]byte{[]byte("m1"), []byte("m2"), []byte("m3")},
	}
	reg := registry.New()
	reg.RegisterSource("ack_test_source", func(id string, _ map[string]any) (stage.Source, error) { return src, nil })
	testutil.Register(reg)

	ir := &topology.TopologyIR{
		Name: "ack-wal-test",
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "ack_test_source"},
			{ID: "out", Kind: topology.KindSink, Type: testutil.SinkTypeCapture},
		},
		Edges: []topology.EdgeIR{{
			From:     "src",
			To:       "out",
			Required: true,
			Buffer:   config.EdgeBufferConfig{Type: "disk", DiskPath: t.TempDir()},
		}},
	}

	ctx := context.Background()
	eng := New(reg)
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}

	waitForAcks(t, src, 3)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("stop with disk edge: %v", err)
	}

	for i, err := range src.ackSnapshot() {
		if err != nil {
			t.Fatalf("ack[%d] error = %v, want nil", i, err)
		}
	}
}

// runSource must chain the AckingSource callback onto any ackFn the source
// already attached to the message, not overwrite it.
func TestRunSourceKeepsSourceAssignedAckFn(t *testing.T) {
	var preSetAcks atomic.Int32
	src := &ackRecordSource{
		Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "ack_test_source"},
		payloads: [][]byte{[]byte("m1")},
	}
	reg := registry.New()
	reg.RegisterSource("ack_test_source", func(id string, _ map[string]any) (stage.Source, error) {
		return &presetAckSource{ackRecordSource: src, fn: func(error) { preSetAcks.Add(1) }}, nil
	})
	testutil.Register(reg)

	ir := &topology.TopologyIR{
		Name: "ack-preset-test",
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "ack_test_source"},
			{ID: "out", Kind: topology.KindSink, Type: testutil.SinkTypeCapture},
		},
		Edges: []topology.EdgeIR{{From: "src", To: "out", Required: true}},
	}

	ctx := context.Background()
	eng := New(reg)
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}

	waitForAcks(t, src, 1)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = eng.Stop(stopCtx)

	if preSetAcks.Load() != 1 {
		t.Fatalf("source-assigned ackFn fired %d times, want 1 (overwritten by SetAckFn)", preSetAcks.Load())
	}
}

// presetAckSource attaches its own ackFn to every message before handing it
// to the engine, simulating sources that track delivery themselves.
type presetAckSource struct {
	*ackRecordSource
	fn func(error)
}

func (s *presetAckSource) Consume(ctx context.Context, out chan<- *message.Message) error {
	for _, p := range s.payloads {
		msg := message.New(p, nil)
		msg.SetAckFn(s.fn)
		select {
		case out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}
