package engine

import (
	"os"
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/store"
)

// The wasm tier (redesign-v3.md §4.5 tier 3) runs the real guest under wazero
// inside the real engine: payload in, JSON stats out, and guest failures
// dead-letter through the same delivery path as Starlark failures.
func guestPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("EVENTBOAT_WASM_GUEST"); p != "" {
		return p
	}
	const p = "../wasmhost/testdata/aggregate.wasm"
	if _, err := os.Stat(p); err != nil {
		t.Skipf("guest not built (%v)", err)
	}
	return p
}

func TestWasmTransformChain(t *testing.T) {
	mod := guestPath(t)
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: wasm-chain }
sources:
  in: { manual: { id: wasm } }
transforms:
  stats:
    from: [in]
    wasm:
      module: "` + mod + `"
sinks:
  out:
    from: [stats]
    mem: { id: out }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("wasm"), h.reg, fastOptions())

	src := h.source("wasm")
	src.Emit([]byte(`{"samples":[1,2,3]}`), "")
	waitSettled(t, eng)

	got, _, _ := h.sink("out").snapshot()
	if len(got) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(got))
	}
	payload := decodeJSON(t, got[0].Out)
	if payload["count"] != float64(3) || payload["max"] != float64(3) {
		t.Fatalf("wasm output wrong: %v", payload)
	}
}

func TestWasmTransformFailureDeadLetters(t *testing.T) {
	mod := guestPath(t)
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: wasm-fail }
sources:
  in: { manual: { id: wasmf } }
transforms:
  stats:
    from:
      in:
        delivery: { retries: 1, backoff: constant }
    wasm:
      module: "` + mod + `"
sinks:
  out:
    from: [stats]
    mem: { id: outf }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("wasm-fail"), h.reg, fastOptions())

	src := h.source("wasmf")
	// Empty values: the guest reports a domain error → retry → dead letter.
	src.Emit([]byte(`{"samples":[]}`), "")
	waitSettled(t, eng)

	if got := eng.Metrics.DeadLettered.Load(); got != 1 {
		t.Fatalf("dead lettered %d, want 1", got)
	}
	if got := eng.Metrics.Retries.Load(); got < 1 {
		t.Fatalf("retries %d, want >= 1 (delivery policy applies to wasm failures)", got)
	}
	_, writes, _ := h.sink("outf").snapshot()
	if writes != 0 {
		t.Fatalf("sink saw %d writes after a failed wasm transform", writes)
	}
	// The dead letter carries the guest's error message.
	dlq, err := eng.Store.DeadLetters("wasm-fail")
	if err != nil || len(dlq) != 1 {
		t.Fatalf("dead letters: %v, %d", err, len(dlq))
	}
	if want := "samples must not be empty"; !strings.Contains(dlq[0].Reason, want) {
		t.Fatalf("dead letter reason %q does not contain %q", dlq[0].Reason, want)
	}
}
