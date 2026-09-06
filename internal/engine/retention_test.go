package engine

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
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
    depends_on: [in]
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
	st := store.NewMemory()

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

// dlqYAML arms dlq.retention through the pipeline config: the engine must
// pick it up without any Options plumbing (the IR-config fallback in New).
const dlqYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: ret }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
dlq:
  retention: 1h
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`

func deadLetterCount(t *testing.T, st store.Store) int {
	t.Helper()
	dls, err := st.DeadLetters("ret")
	if err != nil {
		t.Fatal(err)
	}
	return len(dls)
}

// TestDLQRetentionTrimsExpiredDeadLetters is the end-to-end sweep: messages
// that exhaust delivery dead-letter; once the engine clock moves past the
// configured retention and the durable checkpoint crosses another retention
// window, the expired dead letters are gone (and with them any `replay`
// input they carried — the operator opted in by configuring dlq.retention).
func TestDLQRetentionTrimsExpiredDeadLetters(t *testing.T) {
	h := newHarness(t)
	pip := h.build(dlqYAML)
	st, err := store.OpenSQLite(t.TempDir() + "/dlqret.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// Movable clock starting at real time: dead letters are stamped by the
	// store's wall clock, the cutoff derives from the engine clock, so the
	// sweep only fires once the engine clock runs ahead of the stamps.
	var ahead atomic.Int64
	opts := fastOptions()
	opts.Clock = func() time.Time { return time.Now().Add(time.Duration(ahead.Load())) }
	opts.SpoolRetention = 5

	h.sink("out").fail = func(int) error { return fmt.Errorf("sink down") }
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	// Window 1 (retentionDue 0): the sweep fires with cutoff = now-1h —
	// nothing is expired yet, the fresh dead letter stays.
	h.source("in").Emit([]byte(`{"i":0}`), "")
	waitCommit(t, eng)
	waitFor(t, func() bool { return deadLetterCount(t, st) == 1 })

	// Past the retention horizon. Messages go out ONE at a time, each
	// drained before the next: the sweep runs synchronously on the commit
	// that crosses the retention window, after that message's own dead
	// letter is durable, so the sweep always sees every row written so far
	// (a batch crossing the boundary mid-flight leaves its tail for the next
	// window — harmless at runtime, racy for this assertion).
	ahead.Store(int64(2 * time.Hour))
	for i := 0; i < 10 && deadLetterCount(t, st) > 0; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, 100+i)), "")
		waitCommit(t, eng)
	}
	if n := deadLetterCount(t, st); n != 0 {
		t.Fatalf("expired dead letters survived retention: %d", n)
	}
}

// TestDLQRetentionSelectiveCutoff pins the cutoff semantics: rows strictly
// before clock-retention go, rows at or after it stay. Rows are seeded with
// explicit timestamps so the test stays independent of the wall clock.
func TestDLQRetentionSelectiveCutoff(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML) // no dlq section: the knob is Opts here
	st := store.NewMemory()

	base := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	seed := func(id string, createdAt time.Time) {
		t.Helper()
		if err := st.WriteDeadLetter(store.DeadLetter{Pipeline: "ret", MessageID: id, Node: "out", Reason: "x", Raw: []byte(`{}`), CreatedAt: createdAt}); err != nil {
			t.Fatal(err)
		}
	}
	seed("expired", base.Add(-2*time.Hour))
	seed("fresh", base.Add(-30*time.Minute))

	opts := fastOptions()
	opts.Clock = testkit.FixedClock(base)
	opts.DLQRetention = time.Hour
	opts.SpoolRetention = 5
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	h.source("in").Emit([]byte(`{"i":0}`), "")
	waitCommit(t, eng)

	dls, err := st.DeadLetters("ret")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].MessageID != "fresh" {
		t.Fatalf("dead letters after trim: %+v, want only \"fresh\"", dls)
	}
}

// TestDLQRetentionUnsetKeepsEverything is the keep-forever default: without
// dlq.retention (neither config nor Options) no dead letter is ever pruned,
// however old — replay input is never silently destroyed.
func TestDLQRetentionUnsetKeepsEverything(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML)
	st := store.NewMemory()

	base := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	if err := st.WriteDeadLetter(store.DeadLetter{Pipeline: "ret", MessageID: "ancient", Node: "out", Reason: "x", Raw: []byte(`{}`), CreatedAt: base.Add(-1000 * time.Hour)}); err != nil {
		t.Fatal(err)
	}

	opts := fastOptions()
	opts.Clock = testkit.FixedClock(base)
	opts.SpoolRetention = 5 // cross several retention windows
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	for i := 0; i < 12; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, i)), "")
	}
	waitCommit(t, eng)
	waitFor(t, func() bool {
		cp, _ := st.Checkpoint("ret")
		return cp == 12 // retention windows (0-5, 6-11) have both elapsed
	})

	dls, err := st.DeadLetters("ret")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].MessageID != "ancient" {
		t.Fatalf("unset retention must keep dead letters forever, got %+v", dls)
	}
}

// failingDLQStore fails only the retention sweep: the commit path must ride
// through it (logged, retried next window) like any other retention error.
type failingDLQStore struct {
	store.Store
}

func (s *failingDLQStore) DeleteDeadLettersBefore(pipeline string, cutoff time.Time) (int64, error) {
	return 0, fmt.Errorf("injected sweep failure")
}

// TestDLQRetentionFailureDoesNotBlockCommit: a failing DLQ sweep must not
// wedge the engine — messages still commit and checkpoints still advance
// (the sweep is history hygiene, never a correctness gate).
func TestDLQRetentionFailureDoesNotBlockCommit(t *testing.T) {
	h := newHarness(t)
	pip := h.build(retentionYAML)
	st := &failingDLQStore{Store: store.NewMemory()}

	var logs atomic.Int64
	opts := fastOptions()
	opts.DLQRetention = time.Hour
	opts.Logf = func(format string, args ...any) {
		if strings.Contains(fmt.Sprintf(format, args...), "dlq retention") {
			logs.Add(1)
		}
	}
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	defer stop()

	for i := 0; i < 3; i++ {
		h.source("in").Emit([]byte(fmt.Sprintf(`{"i":%d}`, i)), "")
	}
	waitCommit(t, eng)
	if cp, _ := st.Checkpoint("ret"); cp != 3 {
		t.Fatalf("checkpoint = %d, want 3 (sweep failure must not block commit)", cp)
	}
	if logs.Load() == 0 {
		t.Error("failing dlq sweep was not logged")
	}
}
