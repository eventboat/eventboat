package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

// The pipeline-aggregated admission pool (M2 review R17): engines sharing
// Options.Admission draw from one semaphore, so concurrent runs of a
// pipeline hold max_in_flight IN TOTAL instead of max_in_flight each.
func TestSharedAdmissionPoolCapsConcurrentEngines(t *testing.T) {
	h := newHarness(t)
	const pipelineYAMLA = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: poola }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in1:
    decoder: json
    manual: { id: in1 }
sinks:
  out:
    from: [in1]
    mem: { id: shared }
`
	const pipelineYAMLB = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: poolb }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in2:
    decoder: json
    manual: { id: in2 }
sinks:
  out:
    from: [in2]
    mem: { id: shared }
`
	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	pipA := h.build(pipelineYAMLA)
	pipB := h.build(pipelineYAMLB)
	h.sink("shared").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt <= 2 {
			return gate, true // the first two writes (one per engine) wedge
		}
		return nil, false
	}

	pool := make(chan struct{}, 2)
	optsA := fastOptions()
	optsA.Admission = pool
	optsB := fastOptions()
	optsB.Admission = pool
	engA, stopA := runEngine(t, pipA, store.NewMemory(), h.reg, optsA)
	defer stopA()
	engB, stopB := runEngine(t, pipB, store.NewMemory(), h.reg, optsB)
	defer stopB()

	// One wedged in-flight message per engine: both pool slots are held.
	h.source("in1").Emit([]byte(`{"i":"a1"}`), "")
	h.source("in2").Emit([]byte(`{"i":"b1"}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("shared").snapshot()
		return writes >= 2 // both writes started (wedged on the gate)
	})

	// The pool (cap 2) is exhausted: a third emission must not be accepted
	// until a slot frees. The block is structural — both holds are wedged on
	// the gate, nothing can commit — so the negative window is deterministic.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.source("in1").Emit([]byte(`{"i":"a2"}`), "") // blocks on the pool
	}()
	time.Sleep(150 * time.Millisecond)
	if got := engA.Metrics.MessagesIn.Load(); got != 1 {
		t.Fatalf("third message admitted despite exhausted shared pool (messagesIn=%d)", got)
	}

	// Free both slots: the wedged writes complete, messages commit, the
	// blocked emission proceeds and commits too.
	safeClose(gate)
	wg.Wait()
	waitCommit(t, engA)
	waitCommit(t, engB)
	if got := engA.Metrics.MessagesIn.Load(); got != 2 {
		t.Fatalf("third message never admitted after slot release (messagesIn=%d)", got)
	}
	delivered, _, _ := h.sink("shared").snapshot()
	if len(delivered) != 3 {
		t.Fatalf("delivered %d messages, want 3", len(delivered))
	}
}
