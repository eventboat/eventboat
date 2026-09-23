package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 01 acceptance: crash recovery must replay more uncommitted rows
// than a node channel can hold. Run starts the consumers BEFORE the replay
// scan (startWorkers → replaySpool → startSources), so replay dispatches into
// live channels; before the fix the scan ran with no workers and blocked on
// the first full channel forever.

const replayCapacityYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: replaycap }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`

// spoolRows writes n rows that look exactly like live emissions of node:
// stamped, codec-tagged, attributable to the source.
func spoolRows(t *testing.T, st store.Store, pipeline, node string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m-%04d", i)
		if _, err := st.AppendSpool(pipeline, registry.Message{
			ID:    id,
			Codec: "json",
			Raw:   []byte(fmt.Sprintf(`{"i":%d}`, i)),
			Meta:  map[string]any{"message_id": id, "source": node},
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCrashReplayMoreRowsThanChannelHolds(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayCapacityYAML)
	st := store.NewMemory()
	const rows = 300
	spoolRows(t, st, "replaycap", "in", rows)

	opts := fastOptions()
	opts.DisableSources = true
	opts.ChannelSize = 32 // far below the replay backlog
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	waitCommit(t, eng) // 5s bound: the pre-fix scan hangs here
	if cp, _ := st.Checkpoint("replaycap"); cp != rows {
		t.Fatalf("checkpoint = %d, want %d", cp, rows)
	}
	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != rows {
		t.Fatalf("delivered %d messages, want %d", len(delivered), rows)
	}
	if n := len(eng.admit.gate); n != 0 {
		t.Fatalf("replay left %d admission slot(s) held, want 0", n)
	}
}

// Replay rows take admission quota (decision 4) and release it: the
// uncommitted set is ≤ HighWatermark by construction, so a replay backlog can
// never wedge the gate, and the ledger must be empty once everything commits.
func TestReplayAdmissionQuotaBalances(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayCapacityYAML)
	st := store.NewMemory()
	const rows = 40
	spoolRows(t, st, "replaycap", "in", rows)

	opts := fastOptions()
	opts.DisableSources = true
	opts.ChannelSize = 4
	opts.HighWatermark = 8
	eng, _ := runEngine(t, pip, st, h.reg, opts)
	waitCommit(t, eng)

	if cp, _ := st.Checkpoint("replaycap"); cp != rows {
		t.Fatalf("checkpoint = %d, want %d", cp, rows)
	}
	if n := len(eng.admit.gate); n != 0 {
		t.Fatalf("gate holds %d slot(s) after full commit, want 0", n)
	}
	eng.admit.acquiredMu.Lock()
	leaked := len(eng.admit.acquired)
	eng.admit.acquiredMu.Unlock()
	if leaked != 0 {
		t.Fatalf("acquired ledger holds %d seq(s) after full commit, want 0", leaked)
	}
}

// Replay never exceeds HighWatermark: with the sink wedged and a one-slot
// channel, exactly HighWatermark rows may be admitted; a third would mean the
// gate is not enforced on the replay path.
func TestReplayRespectsHighWatermark(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayCapacityYAML)
	st := store.NewMemory()
	const rows = 10
	spoolRows(t, st, "replaycap", "in", rows)

	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }

	opts := fastOptions()
	opts.DisableSources = true
	opts.ChannelSize = 1
	opts.HighWatermark = 2
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	waitFor(t, func() bool {
		_, _, arrived := eng.CommitSnapshot()
		return arrived == 2
	})
	// The gate is structurally full (the sink write is wedged and nothing can
	// commit), so the negative window is deterministic.
	time.Sleep(100 * time.Millisecond)
	if _, _, arrived := eng.CommitSnapshot(); arrived != 2 {
		t.Fatalf("arrivedMax = %d with a full gate, want 2 (HighWatermark)", arrived)
	}
	if n := len(eng.admit.gate); n != 2 {
		t.Fatalf("gate holds %d slot(s), want 2 (HighWatermark)", n)
	}

	safeClose(gate)
	waitCommit(t, eng)
	if cp, _ := st.Checkpoint("replaycap"); cp != rows {
		t.Fatalf("checkpoint = %d after release, want %d", cp, rows)
	}
	if n := len(eng.admit.gate); n != 0 {
		t.Fatalf("gate holds %d slot(s) after commit, want 0", n)
	}
}

// Abandon force-terminates outstanding messages without a terminal branch
// event, so the commit sweep never fires onCommit for them — the admission
// slot must be released explicitly (candidate 01 fix; it used to leak until
// the gate wedged).
func TestAbandonReleasesAdmissionSlots(t *testing.T) {
	h := newHarness(t)
	pip := h.build(replayCapacityYAML)
	st := store.NewMemory()
	spoolRows(t, st, "replaycap", "in", 2)

	gate := make(chan struct{})
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }

	opts := fastOptions()
	opts.DisableSources = true
	opts.ChannelSize = 4
	opts.HighWatermark = 2
	eng, _ := runEngine(t, pip, st, h.reg, opts)
	// Release the wedged sink before the engine's cleanup waits on drain
	// (cleanups run LIFO).
	t.Cleanup(func() { safeClose(gate) })
	waitFor(t, func() bool {
		_, _, arrived := eng.CommitSnapshot()
		return arrived == 2
	})
	if n := len(eng.admit.gate); n != 2 {
		t.Fatalf("gate holds %d slot(s) before Abandon, want 2", n)
	}

	n, err := eng.Abandon("quota test")
	if err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if n != 2 {
		t.Fatalf("abandoned = %d, want 2", n)
	}
	if got := len(eng.admit.gate); got != 0 {
		t.Fatalf("Abandon left %d admission slot(s) held, want 0", got)
	}
	eng.admit.acquiredMu.Lock()
	leaked := len(eng.admit.acquired)
	eng.admit.acquiredMu.Unlock()
	if leaked != 0 {
		t.Fatalf("Abandon left %d seq(s) in the acquired ledger, want 0", leaked)
	}
}
