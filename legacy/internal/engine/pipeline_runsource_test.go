package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
	"github.com/riverpod/riverpod/internal/stage"
	"github.com/riverpod/riverpod/internal/testutil"
	"github.com/riverpod/riverpod/internal/topology"
)

// burstSource hands all payloads to out at once and returns immediately, so
// errCh and a full out channel are ready at the same time.
type burstSource struct {
	basestage.Base
	payloads [][]byte
	retErr   error
}

func (s *burstSource) Consume(ctx context.Context, out chan<- *message.Message) error {
	for _, p := range s.payloads {
		select {
		case out <- message.New(p, nil):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.retErr
}

func runBurstPipeline(t *testing.T, src *burstSource, sinkID string) *testutil.CaptureSink {
	t.Helper()
	reg := registry.New()
	reg.RegisterSource("burst_source", func(id string, _ map[string]any) (stage.Source, error) { return src, nil })
	testutil.Register(reg)

	ir := &topology.TopologyIR{
		Name: "burst-test-" + sinkID,
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "burst_source"},
			{ID: sinkID, Kind: topology.KindSink, Type: testutil.SinkTypeCapture},
		},
		Edges: []topology.EdgeIR{{From: "src", To: sinkID, Required: true}},
	}

	ctx := context.Background()
	eng := New(reg)
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cap, ok := testutil.CaptureSinkFor(sinkID)
	if !ok {
		t.Fatal("capture sink missing")
	}
	want := len(src.payloads)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(cap.Messages()) < want {
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	return cap
}

// When Consume returns while messages are still buffered in out, runSource
// must not retire early and strand them.
func TestRunSourceDrainsBufferedMessagesAfterConsumeReturns(t *testing.T) {
	payloads := make([][]byte, 64)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("m%02d", i))
	}
	src := &burstSource{
		Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "burst_source"},
		payloads: payloads,
	}
	cap := runBurstPipeline(t, src, "out-clean")
	if got := len(cap.Messages()); got != len(payloads) {
		t.Fatalf("sink got %d messages, want %d (buffered messages stranded on Consume return)", got, len(payloads))
	}
}

// A source error must not discard messages the source already produced.
func TestRunSourceDrainsBufferedMessagesOnSourceError(t *testing.T) {
	payloads := make([][]byte, 64)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("e%02d", i))
	}
	src := &burstSource{
		Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "burst_source"},
		payloads: payloads,
		retErr:   errors.New("source died"),
	}
	cap := runBurstPipeline(t, src, "out-err")
	if got := len(cap.Messages()); got != len(payloads) {
		t.Fatalf("sink got %d messages, want %d (buffered messages stranded on source error)", got, len(payloads))
	}
}
