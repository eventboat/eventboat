package engine

import (
	"context"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// engineCounterSource is a minimal CounterSource: the engine only asserts the
// optional facet and snapshots it.
type engineCounterSource struct {
	counters map[string]int64
}

func (s *engineCounterSource) Init([]byte) error { return nil }
func (s *engineCounterSource) Run(ctx context.Context, emit func(registry.Message) error) error {
	<-ctx.Done()
	return nil
}
func (s *engineCounterSource) Commit(context.Context, int64) ([]byte, error) { return nil, nil }
func (s *engineCounterSource) Close() error                                  { return nil }
func (s *engineCounterSource) Counters() map[string]int64 {
	out := make(map[string]int64, len(s.counters))
	for k, v := range s.counters {
		out[k] = v
	}
	return out
}

// SourceCounters snapshots every source that implements CounterSource and
// omits the ones that do not (the harness's manual source has no facet).
func TestEngineSourceCounters(t *testing.T) {
	h := newHarness(t)
	src := &engineCounterSource{counters: map[string]int64{"lines_read": 5, "rotations": 1}}
	if err := h.reg.RegisterSource("countsrc", 1, manualSchema, nil, func(map[string]any) (registry.Source, error) {
		return src, nil
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: source-counters }
sources:
  counted:
    countsrc: { id: counted }
  plain:
    manual: { id: plain }
sinks:
  out:
    depends_on: [counted, plain]
    mem: { id: out }
`)
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	defer stop()

	counters := eng.SourceCounters()
	if got := counters["counted"]["lines_read"]; got != 5 {
		t.Fatalf("counted.lines_read = %d, want 5", got)
	}
	if got := counters["counted"]["rotations"]; got != 1 {
		t.Fatalf("counted.rotations = %d, want 1", got)
	}
	if _, ok := counters["plain"]; ok {
		t.Fatalf("source without CounterSource appeared in the snapshot: %v", counters)
	}
}
