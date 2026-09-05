package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Spool retention end-to-end: once the durable checkpoint passes the
// retention window, history below it is trimmed — on the SQLite store (disk)
// and the in-memory store (--ephemeral) alike — while everything recovery
// needs stays: the retained window and the uncommitted tail replayed on
// restart (invariant 3 under retention).

const retentionYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: ret }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`

func spoolCount(st store.Store) (count int, first int64) {
	first = -1
	_ = st.ReplayFrom("ret", 0, func(seq int64, m registry.Message, ts time.Time) error {
		if first < 0 {
			first = seq
		}
		count++
		return nil
	})
	return count, first
}

// TestSpoolRetentionTrimsBelowCheckpoint drives 20 commits with a retention
// window of 5: rows 1..15 are history and must go; 16..20 are the retained
// window and must stay — nothing above the cutoff is ever deleted.
func TestSpoolRetentionTrimsBelowCheckpoint(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML)
	st, err := store.OpenSQLite(t.TempDir() + "/ret.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	opts := fastOptions()
	opts.SpoolRetention = 5
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	for i := 0; i < 20; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, i)), "")
	}
	waitCommit(t, eng)
	if cp, _ := st.Checkpoint("ret"); cp != 20 {
		t.Fatalf("checkpoint = %d, want 20", cp)
	}
	waitFor(t, func() bool {
		n, _ := spoolCount(st)
		return n <= 5
	})
	n, first := spoolCount(st)
	if n != 5 || first != 16 {
		t.Fatalf("spool after trim: %d rows starting at %d, want 5 rows starting at 16", n, first)
	}
}

// TestSpoolRetentionKeepsUncommittedTail is invariant 3 with retention
// armed: after history has been trimmed, messages wedged mid-delivery
// (uncommitted, above the checkpoint) must still be replayed by a restarted
// engine — the trim may never catch up to the replay window.
func TestSpoolRetentionKeepsUncommittedTail(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML)
	dbPath := t.TempDir() + "/tail.db"
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })

	// The wedge is armed only after the committed bulk: attempts before it
	// flow normally, every attempt after freezes (a process frozen
	// mid-delivery — exactly the crash this models).
	var armed atomic.Bool
	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if armed.Load() {
			return gate, true
		}
		return nil, false
	}

	opts := fastOptions()
	opts.SpoolRetention = 5
	eng1, _ := runEngine(t, pip, st1, h.reg, opts) // abandoned below, like a crash
	for i := 0; i < 20; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, i)), "")
	}
	waitCommit(t, eng1)
	waitFor(t, func() bool {
		n, _ := spoolCount(st1)
		return n <= 5
	})

	armed.Store(true)
	h.source("in").Emit([]byte(`{"i":"A"}`), "") // wedges mid-delivery
	h.source("in").Emit([]byte(`{"i":"B"}`), "") // queues behind it
	waitFor(t, func() bool {
		outstanding, _, _ := eng1.CommitSnapshot()
		return outstanding >= 2
	})

	st2, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	recovered := &memSink{id: "out2"}
	opts2 := fastOptions()
	opts2.SpoolRetention = 5
	opts2.SinkWrapper = func(node string, s registry.Sink) registry.Sink { return recovered }
	eng2, _ := runEngine(t, pip, st2, h.reg, opts2)
	waitCommit(t, eng2)

	replayed, _, _ := recovered.snapshot()
	if len(replayed) != 2 {
		t.Fatalf("replay delivered %d messages, want exactly the uncommitted tail (2)", len(replayed))
	}
	if body := decodeJSON(t, replayed[0].Out); body["i"] != "A" || decodeJSON(t, replayed[1].Out)["i"] != "B" {
		t.Fatalf("replayed wrong tail: %v %v", replayed[0].Out, replayed[1].Out)
	}
	if cp, _ := st2.Checkpoint("ret"); cp != 22 {
		t.Errorf("checkpoint after recovery = %d, want 22", cp)
	}
}

// TestSpoolRetentionBindsMemoryStore mirrors the bound for --ephemeral runs:
// the in-memory spool must stop growing with total messages and settle at
// the retention window.
func TestSpoolRetentionBindsMemoryStore(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML)
	st := store.NewMemory("ret")

	opts := fastOptions()
	opts.SpoolRetention = 5
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	for i := 0; i < 40; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, i)), "")
	}
	waitCommit(t, eng)
	waitFor(t, func() bool {
		n, _ := spoolCount(st)
		return n <= 5
	})
	n, first := spoolCount(st)
	if n != 5 || first != 36 {
		t.Fatalf("ephemeral spool after trim: %d rows starting at %d, want 5 rows starting at 36", n, first)
	}
	if cp, _ := st.Checkpoint("ret"); cp != 40 {
		t.Fatalf("checkpoint = %d, want 40", cp)
	}
}
