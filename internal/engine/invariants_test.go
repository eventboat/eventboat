package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// The seven reliability invariants of redesign-v3.md §6.2, each locked by one
// dedicated, retrievable test (TestInvariant_*).

const invYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: inv }
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

// Invariant 1: a message must not become visible to the DAG until its spool
// append has succeeded. A failing append means the message is refused — it
// never reaches any sink.
func TestInvariant_SpoolBeforeVisible(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory()}
	failAppend := true
	wrapped.AppendHook = func(m registry.Message) error {
		if failAppend {
			return errString("disk full")
		}
		return nil
	}
	eng, _ := runEngine(t, pip, wrapped, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool { return eng.Metrics.SpoolFailures.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)

	if delivered, _, _ := h.sink("out").snapshot(); len(delivered) != 0 {
		t.Fatalf("message became visible despite failed spool append")
	}
	if out, _, _ := eng.CommitSnapshot(); out != 0 {
		t.Errorf("outstanding = %d, want 0 (message never entered the DAG)", out)
	}

	// Positive control: with the store healthy the same emission flows.
	failAppend = false
	h.source("in").Emit([]byte(`{"i":2}`), "")
	waitCommit(t, eng)
	if delivered, _, _ := h.sink("out").snapshot(); len(delivered) != 1 {
		t.Fatalf("healthy path broken: %d delivered", len(delivered))
	}
}

// Invariant 2: the checkpoint only advances over committed messages. A wedged
// write keeps the message uncommitted and pins the contiguous checkpoint.
func TestInvariant_CheckpointAdvancesOnlyAfterCommit(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	st := store.NewMemory()
	gate := make(chan struct{})
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 2 {
			return gate, true
		}
		return nil, false
	}
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitCommit(t, eng)
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint after first commit = %d, want 1", cp)
	}

	h.source("in").Emit([]byte(`{"i":2}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 2 // second write is now wedged on the gate
	})
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint advanced to %d while message 2 is uncommitted", cp)
	}

	close(gate)
	waitCommit(t, eng)
	if cp, _ := st.Checkpoint("inv"); cp != 2 {
		t.Fatalf("checkpoint = %d after commit, want 2", cp)
	}
}

// Invariant 3: after a kill -9, replay from the checkpoint covers every
// uncommitted message (at-least-once; duplicates are allowed, loss is not).
func TestInvariant_Kill9ReplayReplaysAllUncommitted(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	dbPath := t.TempDir() + "/inv3.db"
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })

	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) }) // release the wedged write at cleanup
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 2 {
			return gate, true // message B wedges: "crash" mid-delivery
		}
		return nil, false
	}
	eng1, _ := runEngine(t, pip, st1, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":"A"}`), "")
	waitCommit(t, eng1)
	h.source("in").Emit([]byte(`{"i":"B"}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 2
	})
	if cp, _ := st1.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint = %d, want 1 (B uncommitted)", cp)
	}

	// Simulate the crash: abandon engine 1 (its wedged write stays wedged —
	// exactly a process frozen mid-delivery) and reopen the store.
	st2, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	// Fresh working sink for the recovered process.
	recovered := &memSink{id: "out2"}
	opts := fastOptions()
	opts.SinkWrapper = func(node string, s registry.Sink) registry.Sink { return recovered }
	eng2, _ := runEngine(t, pip, st2, h.reg, opts)
	waitCommit(t, eng2)

	replayed, _, _ := recovered.snapshot()
	if len(replayed) != 1 {
		t.Fatalf("replay delivered %d messages, want exactly B", len(replayed))
	}
	if body := decodeJSON(t, replayed[0].Out); body["i"] != "B" {
		t.Fatalf("replayed wrong message: %v", body)
	}
	if cp, _ := st2.Checkpoint("inv"); cp != 2 {
		t.Errorf("checkpoint after recovery = %d, want 2", cp)
	}
	// The replayed delivery must preserve the original message identity.
	if replayed[0].Meta["message_id"] == "" {
		t.Error("message_id lost across replay")
	}
}

// Invariant 4: when the dead letter write itself fails, the message must not
// commit — the pipeline degrades (blocks that branch) instead of losing data.
func TestInvariant_DeadLetterWriteFailureBlocksCommit(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory()}
	var dlBroken atomic.Bool
	dlBroken.Store(true)
	wrapped.DeadLetterHook = func(dl store.DeadLetter) error {
		if dlBroken.Load() {
			return errString("dlq unavailable")
		}
		return nil
	}
	h.sink("out").fail = func(attempt int) error { return errString("sink down") }

	st := wrapped
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":1}`), "")

	waitFor(t, func() bool { return eng.Metrics.DlqFailures.Load() >= 2 })
	time.Sleep(50 * time.Millisecond)

	out, committedThrough, _ := eng.CommitSnapshot()
	if out == 0 {
		t.Fatal("message committed despite dead letter write failure")
	}
	if committedThrough != 0 {
		t.Fatalf("checkpoint advanced to %d with uncommitted dead letter", committedThrough)
	}
	dls, _ := st.DeadLetters("inv")
	if len(dls) != 0 {
		t.Fatalf("dead letter recorded despite failing writes: %d", len(dls))
	}

	// Repair the dead letter store: the message commits through the DLQ.
	dlBroken.Store(false)
	waitCommit(t, eng)
	dls, _ = st.DeadLetters("inv")
	if len(dls) != 1 {
		t.Fatalf("dead letter missing after repair: %d", len(dls))
	}
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Errorf("checkpoint = %d after DL commit, want 1", cp)
	}
}

// Invariant 5: a required:false edge that fails affects only its own branch;
// sibling branches commit normally and the message still commits.
func TestInvariant_RequiredFalseFailureDoesNotBlockSiblingBranches(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: inv5 }
edge_defaults:
  delivery: { retries: 1, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  primary:
    from: [in]
    mem: { id: primary }
  telemetry:
    from: { in: { required: false } }
    mem: { id: telemetry }
`)
	st := store.NewMemory()
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())
	h.sink("telemetry").fail = func(attempt int) error { return errString("telemetry down") }

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitCommit(t, eng)

	primary, _, _ := h.sink("primary").snapshot()
	if len(primary) != 1 {
		t.Fatalf("required sibling branch lost the message")
	}
	if got := eng.Metrics.OptionalDrops.Load(); got != 1 {
		t.Errorf("optional drops = %d, want 1", got)
	}
	dls, _ := st.DeadLetters("inv5")
	if len(dls) != 0 {
		t.Fatalf("optional edge failure must not dead letter: %d", len(dls))
	}
	if cp, _ := st.Checkpoint("inv5"); cp != 1 {
		t.Errorf("checkpoint = %d, want 1 (message committed)", cp)
	}
}

// Invariant 6: re-delivering the same message is safe for idempotent sinks —
// meta.message_id stays stable across duplicates and replays.
func TestInvariant_RedeliveryKeepsMessageIdStable(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	// Duplicate delivery of the same raw input: two distinct messages, but
	// each carries a stable idempotency key (meta.message_id) and identical
	// content — what an idempotent sink needs to deduplicate safely.
	st := store.NewMemory()
	eng, stopA := runEngine(t, pip, st, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":1}`), "")
	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitCommit(t, eng)
	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 2 {
		t.Fatalf("want 2 deliveries, got %d", len(delivered))
	}
	id1, _ := delivered[0].Meta["message_id"].(string)
	id2, _ := delivered[1].Meta["message_id"].(string)
	if id1 == "" || id2 == "" {
		t.Fatalf("deliveries lack message_id: %q / %q", id1, id2)
	}
	if string(delivered[0].Out) != string(delivered[1].Out) {
		t.Errorf("duplicate deliveries differ in content")
	}
	stopA() // stop before the next engine: plugin instances are shared

	// Replay duplicate: checkpoint persistence fails, so the committed message
	// is replayed on restart — with the SAME spooled message_id.
	dbPath := t.TempDir() + "/inv6.db"
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })
	noCheckpoint := &testkit.StoreWrapper{Inner: st1}
	noCheckpoint.SetCheckpointHook = func(seq int64) error { return errString("checkpoint write failed") }
	eng1, stop1 := runEngine(t, pip, noCheckpoint, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":9}`), "")
	// Wait for the commit COUNT, not WaitCommit: until the emission is
	// consumed (the engine may still be starting up when Emit returns),
	// outstanding==0 reads as committed and the wait would race — then stop1
	// would cancel mid-emission and the scenario falls apart (flaked under
	// load; found while hardening M3 CI).
	deadline := time.Now().Add(5 * time.Second)
	for eng1.Metrics.CommittedCount.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if eng1.Metrics.CommittedCount.Load() < 1 {
		t.Fatalf("eng1: the emitted message never committed (messagesIn=%d committed=%d dead=%d noMatch=%d decodeErr=%d)",
			eng1.Metrics.MessagesIn.Load(), eng1.Metrics.CommittedCount.Load(), eng1.Metrics.DeadLettered.Load(),
			eng1.Metrics.NoMatch.Load(), eng1.Metrics.DecodeErrors.Load())
	}
	// Stop before reopening the database: eng1's forever-failing checkpoint
	// loop must not race the second connection's reads (this is the crash the
	// scenario models — flaked as an empty replay under load, pre-existing).
	stop1()

	st2, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	recovered := &memSink{id: "out2"}
	opts := fastOptions()
	opts.SinkWrapper = func(node string, s registry.Sink) registry.Sink { return recovered }
	// A visibly different ID generator: if the engine re-stamped the message
	// instead of preserving the spooled ID, the replay would carry fresh-*.
	fresh := 0
	opts.NewID = func() string { fresh++; return fmt.Sprintf("fresh-%03d", fresh) }
	_, stop2 := runEngine(t, pip, st2, h.reg, opts)
	defer stop2()
	// Wait for the replayed delivery itself, not WaitCommit: until the
	// replay registers, outstanding==0 reads as "committed" and the wait races
	// engine startup (flaked under load; the invariant under test is the
	// message_id, so wait for exactly the expected delivery).
	replayDeadline := time.Now().Add(5 * time.Second)
	recovered2, writes2, _ := recovered.snapshot()
	for writes2 < 1 && time.Now().Before(replayDeadline) {
		time.Sleep(2 * time.Millisecond)
		recovered2, writes2, _ = recovered.snapshot()
	}

	replayed := recovered2
	if len(replayed) != 1 {
		t.Fatalf("replay delivered %d, want the 1 uncommitted-checkpoint message", len(replayed))
	}
	replayID, _ := replayed[0].Meta["message_id"].(string)
	if !strings.HasPrefix(replayID, "id-") {
		t.Errorf("replay must preserve the spooled message_id, got %q", replayID)
	}
}

// Invariant 7: a pull source's cursor watermark only advances to the max
// cursor of committed messages — never past an uncommitted one.
func TestInvariant_CursorWatermarkNeverExceedsCommitted(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: inv7 }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`)
	dbPath := t.TempDir() + "/inv7.db"
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	gate := make(chan struct{})
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 2 {
			return gate, true // c2 wedges mid-write
		}
		return nil, false
	}
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "c1")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 1
	})
	h.source("in").Emit([]byte(`{"i":2}`), "c2")
	h.source("in").Emit([]byte(`{"i":3}`), "c3")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 2 // c2 wedged; c3 queued behind it
	})

	// While c2 is uncommitted, the watermark must not pass c1.
	state, _, err := st.SourceState("inv7", "in")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"c1"`) {
		t.Fatalf("watermark state = %s, want c1 (committed max)", state)
	}
	if strings.Contains(string(state), `"c2"`) || strings.Contains(string(state), `"c3"`) {
		t.Fatalf("watermark advanced past uncommitted cursor: %s", state)
	}

	close(gate)
	waitCommit(t, eng)
	state, _, _ = st.SourceState("inv7", "in")
	delivered7, writes7, _ := h.sink("out").snapshot()
	out7, through7, arrived7 := eng.CommitSnapshot()
	t.Logf("after commit: state=%s delivered=%d writes=%d outstanding=%d through=%d arrived=%d",
		state, len(delivered7), writes7, out7, through7, arrived7)
	if !strings.Contains(string(state), `"c3"`) {
		t.Fatalf("watermark state after full commit = %s, want c3", state)
	}
}

// Invariant 8: fan-out sibling branches share a message's underlying
// Decoded/Meta Go maps (deliver shallow-copies the struct per edge), so no
// transform may mutate them in place. Before review-2026-09, the script
// binding's lazy remove(payload, "k0") deleted the key from the shared map,
// racing the sibling sink branch's json encode — a fatal (unrecoverable)
// concurrent map iteration and map write: the message stayed uncommitted and
// crashed the process again on replay. The COW binding must isolate the
// branches: the sink still sees "k0", and nothing crashes.
func TestInvariant_BranchIsolation(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: inv8 }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  scrub:
    from: [in]
    script: |
      remove(payload, "k0")
sinks:
  out:
    from: [in]
    encoder: json
    mem: { id: out }
`)
	st := store.NewMemory()
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	const msgs = 20
	const keys = 3000 // wide maps widen the read/write window on the shared map
	for i := 0; i < msgs; i++ {
		payload := make(map[string]any, keys)
		for k := 0; k < keys; k++ {
			payload[fmt.Sprintf("k%d", k)] = k
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		h.source("in").Emit(raw, "")
	}
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != msgs {
		t.Fatalf("sink delivered %d messages, want %d", len(delivered), msgs)
	}
	for i, m := range delivered {
		if !strings.Contains(string(m.Out), `"k0"`) {
			t.Fatalf("delivery %d: sibling branch polluted — k0 removed from the shared map: %.200s", i, m.Out)
		}
	}
}

// safeClose closes a gate once, tolerating an earlier close (test cleanup of
// wedged writes).
func safeClose(gate chan struct{}) {
	select {
	case <-gate:
		// already closed
	default:
		close(gate)
	}
}
