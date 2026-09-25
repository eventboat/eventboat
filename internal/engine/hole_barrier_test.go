package engine

import (
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// holeYAML gives the BF1 scenario its two independent sources: each emission
// travels through its own admission, so one can sit in the
// AppendSpool→arrived window while the other runs to delivery.
const holeYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: hole }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  a:
    decoder: json
    manual: { id: a }
  b:
    decoder: json
    manual: { id: b }
sinks:
  out:
    depends_on: [a, b]
    mem: { id: out }
`

// gateAppendStore blocks AppendSpool's RETURN after the inner append has
// committed: the row is durable with its spool seq assigned, but admission has
// not registered it (commit.arrived) yet. That is the AppendSpool→arrived
// window the BF1 defect crossed: the checkpoint sweep used to treat the
// missing outstanding entry as committed and could persist a checkpoint above
// a row that was never delivered (a crash then replays from beyond it; a
// no-cursor source loses it forever).
type gateAppendStore struct {
	*testkit.StoreWrapper
	gate      chan struct{}
	blockedAt chan int64
	armed     atomic.Bool
}

func newGateAppendStore(inner store.Store) *gateAppendStore {
	s := &gateAppendStore{
		StoreWrapper: &testkit.StoreWrapper{Inner: inner},
		gate:         make(chan struct{}),
		blockedAt:    make(chan int64, 1),
	}
	s.armed.Store(true)
	return s
}

func (s *gateAppendStore) AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (int64, error) {
	seq, err := s.StoreWrapper.Inner.AppendSpool(pipeline, msg, ingestTime)
	if err != nil {
		return 0, err
	}
	if s.armed.CompareAndSwap(true, false) {
		s.blockedAt <- seq
		<-s.gate
	}
	return seq, nil
}

var _ store.Store = (*gateAppendStore)(nil)

// hasBody reports whether the delivered messages include one whose payload
// carries i=<body>.
func hasBody(msgs []registry.Message, body string) bool {
	for _, m := range msgs {
		var v map[string]any
		if err := json.Unmarshal(m.Out, &v); err == nil && v["i"] == body {
			return true
		}
	}
	return false
}

// TestHoleBarrierCheckpointAndReplay reproduces the adversarial review's BF1
// scenario deterministically and asserts both halves of the fix:
//
//  1. while row A is durable but unregistered, the durable checkpoint must not
//     cross A's seq even though row B runs the whole path and commits (the
//     pre-fix violation: checkpoint=2 covering row seq=1, replay-from-2 sees
//     0 rows);
//  2. a restart on the same SQLite file replays row A (the no-cursor-source
//     safety net), because the checkpoint never passed it;
//  3. releasing A lets both rows commit in the same run, so the barrier does
//     not wedge progress.
func TestHoleBarrierCheckpointAndReplay(t *testing.T) {
	h := newHarness(t)
	pip := h.build(holeYAML)

	dbPath := filepath.Join(t.TempDir(), "hole.db")
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })
	wrapped := newGateAppendStore(st1)
	t.Cleanup(func() { safeClose(wrapped.gate) })

	eng1, stop1 := runEngine(t, pip, wrapped, h.reg, fastOptions())
	defer stop1()

	// Row A: the append commits, then the admission hangs before arrived().
	emitA := make(chan struct{})
	go func() {
		h.source("a").Emit([]byte(`{"i":"A"}`), "cA")
		close(emitA)
	}()
	var seqA int64
	select {
	case seqA = <-wrapped.blockedAt:
	case <-time.After(5 * time.Second):
		t.Fatal("row A never reached the blocked-append window")
	}
	if seqA != 1 {
		t.Fatalf("row A got spool seq %d, want 1 (the gated first append)", seqA)
	}

	// Row B: a different source runs the whole path — spooled, registered,
	// delivered — while A stays unregistered.
	h.source("b").Emit([]byte(`{"i":"B"}`), "cB")
	waitFor(t, func() bool {
		delivered, _, _ := h.sink("out").snapshot()
		return hasBody(delivered, "B")
	})
	// Give any (wrong) checkpoint flush time to land before asserting.
	time.Sleep(50 * time.Millisecond)

	cp, err := st1.Checkpoint("hole")
	if err != nil {
		t.Fatal(err)
	}
	if cp != 0 {
		var rows int
		_ = st1.ReplayFrom("hole", 0, func(int64, registry.Message, time.Time) error { rows++; return nil })
		t.Fatalf("VIOLATION: durable checkpoint=%d covers unregistered row seq=%d (spool holds %d rows); replay-from-checkpoint would skip it", cp, seqA, rows)
	}
	t.Logf("checkpoint while row seq=%d is durable but unregistered: %d (no crossing; row B was delivered)", seqA, cp)

	// Restart on the same SQLite while A is still unregistered: replay starts
	// at the checkpoint and must deliver A (B comes again: duplicates are
	// allowed, loss is not).
	st2, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	recovered := &memSink{id: "out-recovered"}
	opts2 := fastOptions()
	opts2.SinkWrapper = func(node string, s registry.Sink) registry.Sink { return recovered }
	_, stop2 := runEngine(t, pip, st2, h.reg, opts2)
	defer stop2()

	waitFor(t, func() bool {
		delivered, _, _ := recovered.snapshot()
		return hasBody(delivered, "A")
	})
	replayed, _, _ := recovered.snapshot()
	if len(replayed) < 2 {
		t.Fatalf("RESTART: replay delivered %d rows, want row A plus the uncommitted B", len(replayed))
	}
	if !hasBody(replayed, "A") {
		t.Fatalf("RESTART LOSS: replay delivered %d rows without row A", len(replayed))
	}
	t.Logf("restart replay: %d rows delivered, row A present", len(replayed))

	// Release A: the admission registers it and both rows commit in order.
	close(wrapped.gate)
	select {
	case <-emitA:
	case <-time.After(5 * time.Second):
		t.Fatal("row A's emission stayed blocked after the gate opened")
	}
	waitFor(t, func() bool {
		delivered, _, _ := h.sink("out").snapshot()
		return hasBody(delivered, "A") && hasBody(delivered, "B")
	})
	waitFor(t, func() bool {
		cp, err := st1.Checkpoint("hole")
		return err == nil && cp == 2
	})
	if out, through, arrived := eng1.CommitSnapshot(); out != 0 || through != 2 || arrived != 2 {
		t.Fatalf("after release: outstanding=%d through=%d arrived=%d, want 0/2/2", out, through, arrived)
	}
	t.Logf("after releasing A: both rows delivered, checkpoint=2, outstanding=0 (barrier does not wedge)")

	// An undispatchable row (its source node no longer exists) takes the
	// replay path's arrived+done release: the hole barrier must let it clear
	// instead of pinning the prefix forever.
	orphan := filepath.Join(t.TempDir(), "orphan.db")
	ost, err := store.OpenSQLite(orphan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ost.Close() })
	if _, err := ost.AppendSpool("hole", registry.Message{
		ID: "orphan", Codec: "json", Raw: []byte(`{"i":"orphan"}`),
		Meta: map[string]any{"source": "vanished"},
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	opts3 := fastOptions()
	opts3.SinkWrapper = func(node string, s registry.Sink) registry.Sink { return &memSink{id: "orphan-sink"} }
	eng3, _ := runEngine(t, pip, ost, h.reg, opts3)
	waitFor(t, func() bool {
		cp, err := ost.Checkpoint("hole")
		return err == nil && cp == 1
	})
	if out, through, arrived := eng3.CommitSnapshot(); out != 0 || through != 1 || arrived != 1 {
		t.Fatalf("orphan row: outstanding=%d through=%d arrived=%d, want 0/1/1", out, through, arrived)
	}
	t.Logf("undispatchable replay row: checkpoint=1, outstanding=0 (released, not wedged)")
}
