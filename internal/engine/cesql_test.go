package engine

import (
	"testing"

	"github.com/eventboat/eventboat/internal/store"
)

// The opt-in CESQL dialect on real edges (redesign-v3.md §4.7): meta maps to
// context attributes, data.* reaches the payload (documented extension), and
// evaluation errors count as not-passed through the same metrics as CEL.
func TestCesqlEdgePredicate(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: cesql-edges }
sources:
  in: { manual: { id: cesql } }
sinks:
  big:
    from:
      in:
        when: { lang: cesql, expr: "data.region = 'EU' AND data.amount > 100" }
    mem: { id: big }
  us:
    from:
      in:
        when: { lang: cesql, expr: "data.region = 'US'" }
    mem: { id: us }
`)
	eng, _ := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())

	src := h.source("cesql")
	src.Emit([]byte(`{"region":"EU","amount":250}`), "")
	src.Emit([]byte(`{"region":"EU","amount":50}`), "")
	src.Emit([]byte(`{"region":"US","amount":500}`), "")
	src.Emit([]byte(`{"amount":500}`), "") // missing region => evaluation error on both edges: counted, not passed
	waitCommit(t, eng)

	big, _, _ := h.sink("big").snapshot()
	us, _, _ := h.sink("us").snapshot()
	if len(big) != 1 {
		t.Fatalf("big sink got %d messages, want 1 (EU with amount > 100)", len(big))
	}
	if len(us) != 1 {
		t.Fatalf("us sink got %d messages, want 1 (the US one)", len(us))
	}
	if got := eng.Metrics.CelEvalErrors.Load(); got < 2 {
		t.Fatalf("predicate error counter = %d, want >= 2 (missing attribute on both edges)", got)
	}
}
