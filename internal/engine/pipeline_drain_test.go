package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	"github.com/edgesets/edgestream/internal/topology"
)

// drainAckSource records ack results for every emitted message.
type drainAckSource struct {
	basestage.Base
	payloads [][]byte
	mu       sync.Mutex
	acks     []error
}

func (s *drainAckSource) Consume(ctx context.Context, out chan<- *message.Message) error {
	for _, p := range s.payloads {
		select {
		case out <- message.New(p, nil):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Keep the source alive until shutdown so runSource does not race past
	// buffered messages on the errCh return path.
	<-ctx.Done()
	return ctx.Err()
}

func (s *drainAckSource) OnAck(_ *message.Message, err error) {
	s.mu.Lock()
	s.acks = append(s.acks, err)
	s.mu.Unlock()
}

func (s *drainAckSource) ackSnapshot() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.acks...)
}

// strictCtxSink fails any write whose context is already cancelled, and takes
// some time per write so batches are still in flight when shutdown begins.
type strictCtxSink struct {
	basestage.Base
	delay time.Duration
	mu    sync.Mutex
	got   [][]byte
}

func (s *strictCtxSink) Write(ctx context.Context, msgs []*message.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	time.Sleep(s.delay)
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	for _, m := range msgs {
		s.got = append(s.got, m.Payload)
	}
	s.mu.Unlock()
	return nil
}

func (s *strictCtxSink) Flush(context.Context) error { return nil }

func (s *strictCtxSink) written() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.got...)
}

func TestPipelineStop_DrainsFinalBatchWithLiveContext(t *testing.T) {
	src := &drainAckSource{
		Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "drain_test_source"},
		payloads: [][]byte{[]byte("m1"), []byte("m2"), []byte("m3")},
	}
	sink := &strictCtxSink{
		Base:  basestage.Base{IDVal: "out", KindVal: stage.KindSink, TypeVal: "drain_test_sink"},
		delay: 30 * time.Millisecond,
	}

	reg := registry.New()
	reg.RegisterSource("drain_test_source", func(id string, _ map[string]any) (stage.Source, error) { return src, nil })
	reg.RegisterSink("drain_test_sink", func(id string, _ map[string]any) (stage.Sink, error) { return sink, nil })

	ir := &topology.TopologyIR{
		Name:   "drain-test",
		Engine: config.EngineConfig{DrainTimeout: "5s"},
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "drain_test_source"},
			{ID: "out", Kind: topology.KindSink, Type: "drain_test_sink",
				Batch: &config.BatchConfig{Size: 2}},
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

	// Wait until the first batch lands, then shut down while later batches
	// (including the final partial one) are still in flight.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(sink.written()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if got := len(sink.written()); got != 3 {
		t.Fatalf("sink received %d messages, want 3 (final batch lost during drain); acks=%v", got, src.ackSnapshot())
	}
	acks := src.ackSnapshot()
	if len(acks) != 3 {
		t.Fatalf("source got %d acks, want 3", len(acks))
	}
	for i, err := range acks {
		if err != nil {
			t.Fatalf("ack[%d] error = %v, want nil", i, err)
		}
	}
}
