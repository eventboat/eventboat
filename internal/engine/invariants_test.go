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

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory("inv")}
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
	if out, _, _ := eng.SettleSnapshot(); out != 0 {
		t.Errorf("outstanding = %d, want 0 (message never entered the DAG)", out)
	}

	// Positive control: with the store healthy the same emission flows.
	failAppend = false
	h.source("in").Emit([]byte(`{"i":2}`), "")
	waitSettled(t, eng)
	if delivered, _, _ := h.sink("out").snapshot(); len(delivered) != 1 {
		t.Fatalf("healthy path broken: %d delivered", len(delivered))
	}
}

// Invariant 2: the checkpoint only advances over settled messages. A wedged
// write keeps the message unsettled and pins the contiguous checkpoint.
func TestInvariant_CheckpointAdvancesOnlyAfterSettle(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	st := store.NewMemory("inv")
	gate := make(chan struct{})
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 2 {
			return gate, true
		}
		return nil, false
	}
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint after first settle = %d, want 1", cp)
	}

	h.source("in").Emit([]byte(`{"i":2}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 2 // second write is now wedged on the gate
	})
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint advanced to %d while message 2 is unsettled", cp)
	}

	close(gate)
	waitSettled(t, eng)
	if cp, _ := st.Checkpoint("inv"); cp != 2 {
		t.Fatalf("checkpoint = %d after settle, want 2", cp)
	}
}

// Invariant 3: after a kill -9, replay from the checkpoint covers every
// unsettled message (at-least-once; duplicates are allowed, loss is not).
func TestInvariant_Kill9ReplayReplaysAllUnsettled(t *testing.T) {
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
	waitSettled(t, eng1)
	h.source("in").Emit([]byte(`{"i":"B"}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 2
	})
	if cp, _ := st1.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint = %d, want 1 (B unsettled)", cp)
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
	waitSettled(t, eng2)

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
// settle — the pipeline degrades (blocks that branch) instead of losing data.
func TestInvariant_DeadLetterWriteFailureBlocksSettle(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory("inv")}
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

	out, settledThrough, _ := eng.SettleSnapshot()
	if out == 0 {
		t.Fatal("message settled despite dead letter write failure")
	}
	if settledThrough != 0 {
		t.Fatalf("checkpoint advanced to %d with unsettled dead letter", settledThrough)
	}
	dls, _ := st.DeadLetters("inv")
	if len(dls) != 0 {
		t.Fatalf("dead letter recorded despite failing writes: %d", len(dls))
	}

	// Repair the dead letter store: the message settles through the DLQ.
	dlBroken.Store(false)
	waitSettled(t, eng)
	dls, _ = st.DeadLetters("inv")
	if len(dls) != 1 {
		t.Fatalf("dead letter missing after repair: %d", len(dls))
	}
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Errorf("checkpoint = %d after DL settle, want 1", cp)
	}
}

// Invariant 5: a required:false edge that fails affects only its own branch;
// sibling branches settle normally and the message still settles.
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
	st := store.NewMemory("inv5")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())
	h.sink("telemetry").fail = func(attempt int) error { return errString("telemetry down") }

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)

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
		t.Errorf("checkpoint = %d, want 1 (message settled)", cp)
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
	st := store.NewMemory("inv")
	eng, stopA := runEngine(t, pip, st, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":1}`), "")
	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitSettled(t, eng)
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

	// Replay duplicate: checkpoint persistence fails, so the settled message
	// is replayed on restart — with the SAME spooled message_id.
	dbPath := t.TempDir() + "/inv6.db"
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })
	noCheckpoint := &testkit.StoreWrapper{Inner: st1}
	noCheckpoint.SetCheckpointHook = func(seq int64) error { return errString("checkpoint write failed") }
	eng1, _ := runEngine(t, pip, noCheckpoint, h.reg, fastOptions())
	h.source("in").Emit([]byte(`{"i":9}`), "")
	waitSettled(t, eng1)

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
	eng2, _ := runEngine(t, pip, st2, h.reg, opts)
	waitSettled(t, eng2)

	replayed, _, _ := recovered.snapshot()
	if len(replayed) != 1 {
		t.Fatalf("replay delivered %d, want the 1 unsettled-checkpoint message", len(replayed))
	}
	replayID, _ := replayed[0].Meta["message_id"].(string)
	if !strings.HasPrefix(replayID, "id-") {
		t.Errorf("replay must preserve the spooled message_id, got %q", replayID)
	}
}

// Invariant 7: a pull source's cursor watermark only advances to the max
// cursor of settled messages — never past an unsettled one.
func TestInvariant_CursorWatermarkNeverExceedsSettled(t *testing.T) {
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

	// While c2 is unsettled, the watermark must not pass c1.
	state, _, err := st.SourceState("inv7", "in")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"c1"`) {
		t.Fatalf("watermark state = %s, want c1 (settled max)", state)
	}
	if strings.Contains(string(state), `"c2"`) || strings.Contains(string(state), `"c3"`) {
		t.Fatalf("watermark advanced past unsettled cursor: %s", state)
	}

	close(gate)
	waitSettled(t, eng)
	state, _, _ = st.SourceState("inv7", "in")
	delivered7, writes7, _ := h.sink("out").snapshot()
	out7, through7, arrived7 := eng.SettleSnapshot()
	t.Logf("after settle: state=%s delivered=%d writes=%d outstanding=%d through=%d arrived=%d",
		state, len(delivered7), writes7, out7, through7, arrived7)
	if !strings.Contains(string(state), `"c3"`) {
		t.Fatalf("watermark state after full settle = %s, want c3", state)
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
