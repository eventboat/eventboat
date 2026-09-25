package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// TestAppendCancelDuringAppendRegistersRow closes the orphan-row hazard of the
// group-commit design (2026-09-24 §2.2): AppendSpool takes no ctx and has no
// "give up halfway" path, so a source cancelled while its append is in flight
// still blocks until the batch commits, and admission then registers the
// returned seq unconditionally. A committed row can therefore never be
// invisible to the commit tracker — which would pin the checkpoint's
// contiguous prefix forever (an orphan) — and a shutdown mid-append leaves
// the row spooled and uncommitted, so the next run replays it.
//
// This complements invariant 3 (kill -9 replay) with the cancellation edge of
// the same guarantee; it does not modify any TestInvariant_* test.
func TestAppendCancelDuringAppendRegistersRow(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	// A real store so the row's durability is checkable at the end.
	st, err := store.OpenSQLite(t.TempDir() + "/cancel.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	wrapped := &testkit.StoreWrapper{Inner: st}
	wrapped.AppendHook = func(registry.Message) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	eng, stop := runEngine(t, pip, wrapped, h.reg, fastOptions())

	emitted := make(chan struct{})
	go func() {
		h.source("in").Emit([]byte(`{"i":1}`), "")
		close(emitted)
	}()
	<-entered // the emission is blocked inside AppendSpool

	// Stop the engine while the append is in flight. stop is once-guarded and
	// registered with t.Cleanup too, so the second call is safe.
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	deadline := time.Now().Add(2 * time.Second)
	for eng.ctx.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if eng.ctx.Err() == nil {
		t.Fatal("engine did not cancel while the append was blocked")
	}

	// Let the batch commit: the append succeeds (no cancel path) and
	// admission must register the seq even though the run is shutting down.
	close(release)
	select {
	case <-emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("emission stayed blocked after the append was released")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not stop after the append was released")
	}

	outstanding, committedThrough, arrivedMax := eng.CommitSnapshot()
	if arrivedMax < 1 {
		t.Fatalf("spooled row never registered with the commit tracker (arrivedMax=%d): the next run cannot tell it exists", arrivedMax)
	}

	// The row is durable and covered by replay from the checkpoint.
	var replayed int
	if err := st.ReplayFrom("inv", 0, func(int64, registry.Message, time.Time) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replayed != 1 {
		t.Fatalf("spool rows = %d, want the 1 accepted row", replayed)
	}
	if committedThrough >= arrivedMax {
		// The sink won the shutdown race and committed the row: it went
		// through the normal terminal path and there is nothing to replay.
		if committedThrough != arrivedMax {
			t.Fatalf("committed prefix charged past the registered row: through=%d arrivedMax=%d", committedThrough, arrivedMax)
		}
		return
	}
	// The usual outcome: shutdown dropped the dispatch, so the row is
	// uncommitted and must be pinned by an outstanding branch — never an
	// invisible row past the checkpoint. The checkpoint must not have
	// advanced over it, so replay from 0 (checked above) is the safety net.
	if outstanding == 0 {
		t.Fatalf("uncommitted row left no outstanding branch (through=%d arrivedMax=%d): orphan row", committedThrough, arrivedMax)
	}
}
