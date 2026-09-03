package engine

import (
	"context"
	"errors"
	"sync"
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

// failSink rejects every write.
type failSink struct {
	basestage.Base
}

func (s *failSink) Write(context.Context, []*message.Message) error {
	return errors.New("write boom")
}
func (s *failSink) Flush(context.Context) error { return nil }

// dlqRecSink records DLQ deliveries and whether Write got a bounded context.
type dlqRecSink struct {
	basestage.Base
	mu          sync.Mutex
	msgs        []*message.Message
	sawDeadline bool
	failWrite   bool
}

func (s *dlqRecSink) Write(ctx context.Context, msgs []*message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := ctx.Deadline(); ok {
		s.sawDeadline = true
	}
	if s.failWrite {
		return errors.New("dlq write boom")
	}
	for _, m := range msgs {
		s.msgs = append(s.msgs, m.ShallowCopy())
	}
	return nil
}
func (s *dlqRecSink) Flush(context.Context) error { return nil }

func (s *dlqRecSink) messages() []*message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*message.Message(nil), s.msgs...)
}

func (s *dlqRecSink) deadlineSeen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawDeadline
}

type dlqFixture struct {
	edgeDLQ *dlqRecSink
	plDLQ   *dlqRecSink
	src     *ackRecordSource
}

func newDLQFixture(t *testing.T, edge config.EdgeBufferConfig, delivery *config.DeliverySpec, pipelineDLQ bool) (*dlqFixture, *topology.TopologyIR, *registry.Registry) {
	t.Helper()
	f := &dlqFixture{
		edgeDLQ: &dlqRecSink{Base: basestage.Base{IDVal: "edge-dlq", KindVal: stage.KindSink, TypeVal: "dlq_rec"}},
		plDLQ:   &dlqRecSink{Base: basestage.Base{IDVal: "pl-dlq", KindVal: stage.KindSink, TypeVal: "dlq_rec"}},
		src: &ackRecordSource{
			Base:     basestage.Base{IDVal: "src", KindVal: stage.KindSource, TypeVal: "dlq_ack_source"},
			payloads: [][]byte{[]byte(`{"v":1}`)},
		},
	}
	reg := registry.New()
	testutil.Register(reg)
	reg.RegisterSource("dlq_ack_source", func(id string, _ map[string]any) (stage.Source, error) {
		return f.src, nil
	})
	reg.RegisterSink("fail_sink", func(id string, _ map[string]any) (stage.Sink, error) {
		return &failSink{Base: basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "fail_sink"}}, nil
	})
	reg.RegisterSink("dlq_rec", func(id string, _ map[string]any) (stage.Sink, error) {
		if id == "edge-dlq" {
			return f.edgeDLQ, nil
		}
		return f.plDLQ, nil
	})

	ir := &topology.TopologyIR{
		Name: "dlq-test",
		Stages: []topology.StageIR{
			{ID: "src", Kind: topology.KindSource, Type: "dlq_ack_source"},
			{ID: "out", Kind: topology.KindSink, Type: "fail_sink"},
			{ID: "edge-dlq", Kind: topology.KindSink, Type: "dlq_rec"},
			{ID: "pl-dlq", Kind: topology.KindSink, Type: "dlq_rec"},
		},
		Edges: []topology.EdgeIR{{
			From: "src", To: "out", Required: true,
			Buffer: edge, Delivery: delivery,
		}},
	}
	if pipelineDLQ {
		ir.DLQ = &config.DLQConfig{Sink: "pl-dlq"}
	}
	return f, ir, reg
}

func runDLQPipeline(t *testing.T, reg *registry.Registry, ir *topology.TopologyIR, f *dlqFixture) {
	t.Helper()
	ctx := context.Background()
	eng := New(reg)
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(f.edgeDLQ.messages())+len(f.plDLQ.messages()) > 0 || f.src.ackCount() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// Edge delivery.dlq wins over the pipeline-level dlq.sink.
func TestDeliverToDLQPrefersEdgeDLQ(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, &config.DeliverySpec{DLQ: "edge-dlq"}, true)
	runDLQPipeline(t, reg, ir, f)

	if got := len(f.plDLQ.messages()); got != 0 {
		t.Fatalf("pipeline DLQ got %d messages, want 0 (edge DLQ must win)", got)
	}
	msgs := f.edgeDLQ.messages()
	if len(msgs) != 1 {
		t.Fatalf("edge DLQ got %d messages, want 1", len(msgs))
	}
	meta := msgs[0].Metadata
	assertDLQMetadata(t, meta, "0")
}

// Without an edge DLQ the pipeline-level dlq.sink is the fallback.
func TestDeliverToDLQFallsBackToPipelineDLQ(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, nil, true)
	runDLQPipeline(t, reg, ir, f)

	if got := len(f.edgeDLQ.messages()); got != 0 {
		t.Fatalf("edge DLQ got %d messages, want 0", got)
	}
	msgs := f.plDLQ.messages()
	if len(msgs) != 1 {
		t.Fatalf("pipeline DLQ got %d messages, want 1", len(msgs))
	}
	assertDLQMetadata(t, msgs[0].Metadata, "0")
}

// er-retry-count must reflect the delivery retry policy actually attempted.
func TestDeliverToDLQRetryCount(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, &config.DeliverySpec{
		DLQ:   "edge-dlq",
		Retry: &config.RetryConfig{Max: 2, Backoff: "fixed"},
	}, true)
	runDLQPipeline(t, reg, ir, f)

	msgs := f.edgeDLQ.messages()
	if len(msgs) != 1 {
		t.Fatalf("edge DLQ got %d messages, want 1", len(msgs))
	}
	if got := msgs[0].Metadata["er-retry-count"]; got != "2" {
		t.Fatalf("er-retry-count = %v, want \"2\"", got)
	}
}

// DLQ writes must run under a bounded, cancellable context — not Background.
func TestDeliverToDLQWriteContextIsBounded(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, &config.DeliverySpec{DLQ: "edge-dlq"}, false)
	runDLQPipeline(t, reg, ir, f)

	if len(f.edgeDLQ.messages()) != 1 {
		t.Fatalf("edge DLQ got %d messages, want 1", len(f.edgeDLQ.messages()))
	}
	if !f.edgeDLQ.deadlineSeen() {
		t.Fatal("dlqSink.Write ran under context.Background(); want a bounded context")
	}
}

// er-original-source must survive a disk-edge WAL roundtrip.
func TestDeliverToDLQOriginalSourceSurvivesDiskEdge(t *testing.T) {
	edge := config.EdgeBufferConfig{Type: "disk", DiskPath: t.TempDir()}
	f, ir, reg := newDLQFixture(t, edge, &config.DeliverySpec{DLQ: "edge-dlq"}, false)
	runDLQPipeline(t, reg, ir, f)

	msgs := f.edgeDLQ.messages()
	if len(msgs) != 1 {
		t.Fatalf("edge DLQ got %d messages, want 1", len(msgs))
	}
	if got := msgs[0].Metadata["er-original-source"]; got != "src" {
		t.Fatalf("er-original-source = %v, want \"src\" after WAL roundtrip", got)
	}
}

func assertDLQMetadata(t *testing.T, meta map[string]any, wantRetries string) {
	t.Helper()
	if meta["er-error-type"] != "sink_write_error" {
		t.Fatalf("er-error-type = %v, want sink_write_error", meta["er-error-type"])
	}
	if meta["er-error-stage"] != "out" {
		t.Fatalf("er-error-stage = %v, want out", meta["er-error-stage"])
	}
	if meta["er-original-pipeline"] != "dlq-test" {
		t.Fatalf("er-original-pipeline = %v, want dlq-test", meta["er-original-pipeline"])
	}
	if meta["er-original-source"] != "src" {
		t.Fatalf("er-original-source = %v, want src", meta["er-original-source"])
	}
	if meta["er-retry-count"] != wantRetries {
		t.Fatalf("er-retry-count = %v, want %q", meta["er-retry-count"], wantRetries)
	}
	if r, _ := meta["er-error-reason"].(string); r == "" {
		t.Fatal("er-error-reason missing")
	}
	ts, _ := meta["er-error-timestamp"].(string)
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		t.Fatalf("er-error-timestamp %q not RFC3339: %v", ts, err)
	}
}

// assertDLQAckNil checks that the original message was acked with nil after
// a successful DLQ write: the failure reached its terminal disposition, so
// the source must commit and not redeliver (Kafka Connect semantics).
func assertDLQAckNil(t *testing.T, f *dlqFixture) {
	t.Helper()
	acks := f.src.ackSnapshot()
	if len(acks) != 1 {
		t.Fatalf("source got %d OnAck callbacks, want 1", len(acks))
	}
	if acks[0] != nil {
		t.Fatalf("source ack error = %v, want nil after DLQ success", acks[0])
	}
}

// assertDLQAckErr checks the original message kept its nack: the failure was
// dropped (no DLQ) or the DLQ write itself failed, so the source must not
// commit and should redeliver.
func assertDLQAckErr(t *testing.T, f *dlqFixture) {
	t.Helper()
	acks := f.src.ackSnapshot()
	if len(acks) != 1 {
		t.Fatalf("source got %d OnAck callbacks, want 1", len(acks))
	}
	if acks[0] == nil {
		t.Fatal("source ack error = nil, want non-nil (message was not disposed of)")
	}
}

// A successful edge-level DLQ write is the terminal disposition: the
// original message must be acked with nil so kafka-style sources commit.
func TestDeliverToDLQAcksNilAfterEdgeDLQSuccess(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, &config.DeliverySpec{DLQ: "edge-dlq"}, false)
	runDLQPipeline(t, reg, ir, f)

	if len(f.edgeDLQ.messages()) != 1 {
		t.Fatalf("edge DLQ got %d messages, want 1", len(f.edgeDLQ.messages()))
	}
	assertDLQAckNil(t, f)
}

// Same terminal-disposition semantics for the pipeline-level dlq.sink.
func TestDeliverToDLQAcksNilAfterPipelineDLQSuccess(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, nil, true)
	runDLQPipeline(t, reg, ir, f)

	if len(f.plDLQ.messages()) != 1 {
		t.Fatalf("pipeline DLQ got %d messages, want 1", len(f.plDLQ.messages()))
	}
	assertDLQAckNil(t, f)
}

// A DLQ write failure is not a terminal disposition: the original message
// keeps its nack so the source redelivers.
func TestDeliverToDLQKeepsNackWhenDLQWriteFails(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, &config.DeliverySpec{DLQ: "edge-dlq"}, false)
	f.edgeDLQ.failWrite = true
	runDLQPipeline(t, reg, ir, f)

	if got := len(f.edgeDLQ.messages()); got != 0 {
		t.Fatalf("edge DLQ recorded %d messages, want 0", got)
	}
	assertDLQAckErr(t, f)
}

// Without any DLQ configured the message is dropped and keeps its nack.
func TestDeliverToDLQKeepsNackWithoutDLQ(t *testing.T) {
	f, ir, reg := newDLQFixture(t, config.EdgeBufferConfig{}, nil, false)
	runDLQPipeline(t, reg, ir, f)

	if got := len(f.plDLQ.messages()); got != 0 {
		t.Fatalf("pipeline DLQ recorded %d messages, want 0", got)
	}
	assertDLQAckErr(t, f)
}
