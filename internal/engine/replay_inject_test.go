package engine

import (
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Regression (review-2026-09): a row spooled by InjectAt right before a crash
// must replay exactly the way the live injection ran — INTO its node, not as
// a fan-out from it.

const replayInjectYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: replayinject }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  bump:
    from: [in]
    script: |
      payload.n = payload.n + 1
sinks:
  out:
    from: [bump]
    encoder: json
    mem: { id: out }
`

// spoolInjected appends a row that looks exactly like what injectAt spooled:
// stamped, codec-tagged, attributed to its entry node.
func spoolInjected(t *testing.T, st store.Store, pipeline, node, id, raw string) {
	t.Helper()
	_, err := st.AppendSpool(pipeline, registry.Message{
		ID:    id,
		Codec: "json",
		Raw:   []byte(raw),
		Meta: map[string]any{
			"message_id":  id,
			"source":      node,
			"injected_at": node,
		},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestReplayInjectedTransformRunsScript(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayInjectYAML)
	st := store.NewMemory()
	// Simulate "spooled, never processed": written past no checkpoint, so Run
	// replays it. Before the fix this fanned OUT of `bump` and delivered the
	// raw payload to the sink, skipping the script.
	spoolInjected(t, st, "replayinject", "bump", "inj-1", `{"n":1}`)

	opts := fastOptions()
	opts.DisableSources = true
	eng, _ := runEngine(t, pip, st, h.reg, opts)
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(delivered))
	}
	payload := decodeJSON(t, delivered[0].Out)
	if payload["n"] != 2.0 {
		t.Errorf("transform did not run on replay: n = %v (want 2), out=%s", payload["n"], delivered[0].Out)
	}
}

func TestReplayInjectedSinkMessageReachesSink(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayInjectYAML)
	st := store.NewMemory()
	// Before the fix a sink injection replayed as a fan-out from the sink,
	// which has no out-edges, and was dropped as NoMatch/filtered.
	spoolInjected(t, st, "replayinject", "out", "inj-2", `{"n":10}`)

	opts := fastOptions()
	opts.DisableSources = true
	eng, _ := runEngine(t, pip, st, h.reg, opts)
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(delivered))
	}
	if payload := decodeJSON(t, delivered[0].Out); payload["n"] != 10.0 {
		t.Errorf("payload = %v, want the injected 10", payload["n"])
	}
	if n := eng.Metrics.NoMatch.Load(); n != 0 {
		t.Errorf("NoMatch = %d, injected-into-sink rows must commit, not filter", n)
	}
	if cp, _ := st.Checkpoint("replayinject"); cp != 1 {
		t.Errorf("checkpoint = %d, want 1 (row committed)", cp)
	}
}

func TestNewNormalizesWasmSlowCallWarnMs(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayInjectYAML)
	// Hand-built options (no DefaultOptions): 0 must fall back to the
	// documented 5000ms watchdog default, like every other numeric option.
	eng, err := New(pip, store.NewMemory(), h.reg, Options{
		Clock: time.Now,
		NewID: func() string { return "x" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if eng.Opts.WasmSlowCallWarnMs != 5000 {
		t.Errorf("WasmSlowCallWarnMs = %d, want 5000", eng.Opts.WasmSlowCallWarnMs)
	}
}
