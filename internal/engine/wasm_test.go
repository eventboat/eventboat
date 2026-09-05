package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/wasmhost"
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
	eng, _ := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())

	src := h.source("wasm")
	src.Emit([]byte(`{"samples":[1,2,3]}`), "")
	waitCommit(t, eng)

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
	eng, _ := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())

	src := h.source("wasmf")
	// Empty values: the guest reports a domain error → retry → dead letter.
	src.Emit([]byte(`{"samples":[]}`), "")
	waitCommit(t, eng)

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

// M3-audit J2: fast mode is the default, so an unset wasm.timeout_ms warns
// at verify (--strict upgrades warnings to errors); an explicitly set value
// — either budget or fast — does not.
func TestWasmNoKillSwitchLint(t *testing.T) {
	mod := guestPath(t)
	h := newHarness(t)
	buildDiags := func(wasmExtra string) []config.Diagnostic {
		t.Helper()
		lr := config.LoadBytes("wasm-lint.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: wasm-lint }
sources:
  in: { manual: { id: wl } }
transforms:
  heavy:
    from: [in]
    wasm:
      module: "`+mod+`"
      `+wasmExtra+`
sinks:
  out:
    from: [heavy]
    mem: { id: wout }
`))
		if lr.HasErrors() {
			t.Fatalf("config errors: %+v", lr.Diagnostics)
		}
		_, diags := ir.Build(lr.Pipeline, h.reg, starhost.DefaultOptions(), nil)
		return diags
	}

	has := func(diags []config.Diagnostic, code string) bool {
		for _, d := range diags {
			if d.Code == code {
				return true
			}
		}
		return false
	}

	// Unset: warning (upgradeable to error by --strict).
	if diags := buildDiags(""); !has(diags, "wasm_no_kill_switch") {
		t.Fatalf("unset timeout_ms: want wasm_no_kill_switch warning, got %+v", diags)
	}
	// Explicit positive budget: no warning.
	if diags := buildDiags("timeout_ms: 500"); has(diags, "wasm_no_kill_switch") {
		t.Fatalf("explicit budget must not warn: %+v", diags)
	}
	// Explicit fast mode: no warning (the user chose).
	if diags := buildDiags("timeout_ms: 0"); has(diags, "wasm_no_kill_switch") {
		t.Fatalf("explicit fast mode must not warn: %+v", diags)
	}
}

// TestWasmSlowCallWatchdog covers the zero-interference slow-call log: one
// warning per long-running invoke, none for short calls, never a kill.
func TestWasmSlowCallWatchdog(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a multi-second guest computation")
	}
	path := guestPath(t)
	ctx := context.Background()
	compiled, err := wasmhost.Compile(ctx, path, &wasmhost.Config{}) // fast: no kill switch
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = compiled.Close(ctx) }()

	var mu sync.Mutex
	var logs []string
	logf := func(f string, a ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(f, a...))
		mu.Unlock()
	}

	// Short-call phase: a 5s threshold leaves ample headroom even for a cold
	// first invoke (instantiation + JSON) on a slow CI box under -race.
	calm := compiled.NewInvoker(&wasmhost.Config{}, logf, 5000)
	defer func() { _ = calm.Close() }()
	if _, err := calm.Invoke(ctx, []byte(`{"samples":[1,2,3]}`)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(logs) != 0 {
		mu.Unlock()
		t.Fatalf("short call produced watchdog logs: %v", logs)
	}
	mu.Unlock()

	// Long-call phase: a 1ms threshold guarantees the watchdog fires; the
	// call (10M float ops) must still complete — fast mode never kills — and
	// the warning must fire exactly once (throttled per call).
	hot := compiled.NewInvoker(&wasmhost.Config{}, logf, 1)
	defer func() { _ = hot.Close() }()
	values := make([]float64, 200_000)
	in, _ := json.Marshal(map[string]any{"samples": values, "passes": 50})
	if _, err := hot.Invoke(ctx, in); err != nil {
		t.Fatalf("fast-mode invoke must complete: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	for time.Now().Before(deadline) {
		if countWatchdogLogs(logs) >= 1 {
			break
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
	}
	if n := countWatchdogLogs(logs); n != 1 {
		t.Fatalf("watchdog fired %d times, want exactly 1 (throttled per call); logs=%v", n, logs)
	}
	for _, l := range logs {
		if strings.Contains(l, "still running") && !strings.Contains(l, "transform") {
			t.Errorf("watchdog log misses the entrypoint: %q", l)
		}
	}
}

func countWatchdogLogs(logs []string) int {
	n := 0
	for _, l := range logs {
		if strings.Contains(l, "still running") {
			n++
		}
	}
	return n
}
