package engine

import (
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/store"
)

func spanRecorder() (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	rec := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(rec)),
	)
	return rec, tp
}

func spanAttr(s tracetest.SpanStub, key string) (string, bool) {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

func requireSpanAttr(t *testing.T, s tracetest.SpanStub, key, want string) {
	t.Helper()
	if v, ok := spanAttr(s, key); !ok || v != want {
		t.Fatalf("span attr %q = %q (found %v), want %q", key, v, ok, want)
	}
}

// Per-message span sampling (§6.6, review R16: opt-in via the pipeline's
// telemetry.span_sample_rate). Rate 1 emits one span per accepted message,
// ended at its terminal state; rate 0 (default) emits nothing.
func TestMessageSpanSampling(t *testing.T) {
	rec, tp := spanRecorder()
	o, err := obs.NewWithTracer(tp)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t)
	pip := h.build(invYAML)
	opts := fastOptions()
	opts.Obs = o
	opts.SpanSampleRate = 1
	eng, _ := runEngine(t, pip, store.NewMemory("inv"), h.reg, opts)

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)

	spans := rec.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1: %+v", len(spans), spans)
	}
	if spans[0].Name != "eventboat.message" {
		t.Fatalf("span name = %q", spans[0].Name)
	}
	requireSpanAttr(t, spans[0], "eventboat.pipeline", "inv")
	requireSpanAttr(t, spans[0], "eventboat.terminal_state", "settled")
	if _, ok := spanAttr(spans[0], "eventboat.message_id"); !ok {
		t.Fatalf("span lacks message_id: %+v", spans[0].Attributes)
	}
}

// A dead-lettered message ends its span with the dead_letter terminal state
// and the reason attribute.
func TestMessageSpanDeadLetterTerminal(t *testing.T) {
	rec, tp := spanRecorder()
	o, err := obs.NewWithTracer(tp)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t)
	pip := h.build(invYAML)
	opts := fastOptions()
	opts.Obs = o
	opts.SpanSampleRate = 1
	st := store.NewMemory("inv")
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	h.sink("out").fail = func(attempt int) error { return errString("sink down") }
	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)

	spans := rec.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	requireSpanAttr(t, spans[0], "eventboat.terminal_state", "dead_letter")
	if v, ok := spanAttr(spans[0], "eventboat.error"); !ok || v != "delivery: sink write failed after retries" {
		t.Fatalf("span error attr = %q (found %v)", v, ok)
	}
}

// The default (rate 0) emits nothing — zero cost, R16 semantics unchanged.
func TestMessageSpanSamplingDefaultOff(t *testing.T) {
	rec, tp := spanRecorder()
	o, err := obs.NewWithTracer(tp)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t)
	pip := h.build(invYAML)
	opts := fastOptions()
	opts.Obs = o
	eng, _ := runEngine(t, pip, store.NewMemory("inv"), h.reg, opts)

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)
	if n := len(rec.GetSpans()); n != 0 {
		t.Fatalf("rate 0 emitted %d spans, want 0", n)
	}
}
