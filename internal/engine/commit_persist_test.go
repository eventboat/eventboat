package engine

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// The beta-round out-of-lock persistence refactor (commit callbacks no longer
// hold the tracker mutex across store IO) needs its own guards locked by
// tests beyond the seven invariants (which stay untouched):
//
//  1. concurrent commits flushing out of order must never regress the
//     checkpoint or per-source frontiers (monotonic guards under persistMu);
//  2. the visibility barrier (durableThrough) must not wedge observers when
//     persistence keeps failing — the flush position is consumed on failure,
//     durability itself is retried by the next advance.

const persistYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: perst }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out1: { from: [in], mem: { id: p1 } }
  out2: { from: [in], mem: { id: p2 } }
  out3: { from: [in], mem: { id: p3 } }
`

// TestCommitFlushOutOfLockUnderConcurrency drives concurrent commits through
// a slow, jittering checkpoint write (simulated fsync): four sink workers
// flush advances in arbitrary interleaving while emitters keep arriving.
func TestCommitFlushOutOfLockUnderConcurrency(t *testing.T) {
	h := newHarness(t)
	pip := h.build(persistYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory("perst")}
	var mu sync.Mutex
	var checkpointWrites []int64
	wrapped.SetCheckpointHook = func(seq int64) error {
		// Jittered "fsync": the exact interleaving that used to convoy on
		// the tracker lock.
		time.Sleep(time.Duration(50+rand.Intn(150)) * time.Microsecond)
		mu.Lock()
		checkpointWrites = append(checkpointWrites, seq)
		mu.Unlock()
		return nil
	}
	opts := fastOptions()
	opts.HighWatermark = 256
	eng, stop := runEngine(t, pip, wrapped, h.reg, opts)
	defer stop()

	const emitters = 8
	const perEmitter = 25
	var wg sync.WaitGroup
	for g := 0; g < emitters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perEmitter; i++ {
				h.source("in").Emit([]byte(`{"i":1}`), "")
			}
		}()
	}
	wg.Wait()
	waitCommit(t, eng)

	total := int64(emitters * perEmitter)
	if cp, _ := wrapped.Checkpoint("perst"); cp != total {
		t.Fatalf("checkpoint = %d, want %d", cp, total)
	}
	if got := eng.Metrics.CheckpointPtr.Load(); got != total {
		t.Fatalf("CheckpointPtr = %d, want %d", got, total)
	}
	// The monotonic guard must keep durable checkpoint writes strictly
	// increasing even though flushes race.
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(checkpointWrites); i++ {
		if checkpointWrites[i] <= checkpointWrites[i-1] {
			t.Fatalf("checkpoint write regressed: %d after %d (writes: %v)",
				checkpointWrites[i], checkpointWrites[i-1], checkpointWrites)
		}
	}
}

// TestCommitFlushFailureDoesNotBlockWaitCommit locks the barrier semantics
// for a permanently failing checkpoint store: WaitCommit observes the flush
// ATTEMPT, not its success — a failed persist widens the replay window
// (invariant 3 covers it) but must not wedge quiescence detection.
func TestCommitFlushFailureDoesNotBlockWaitCommit(t *testing.T) {
	h := newHarness(t)
	pip := h.build(persistYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory("perst")}
	wrapped.SetCheckpointHook = func(seq int64) error { return errString("disk gone") }
	eng, stop := runEngine(t, pip, wrapped, h.reg, fastOptions())
	defer stop()

	h.source("in").Emit([]byte(`{"i":1}`), "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.WaitCommit(ctx); err != nil {
		t.Fatalf("WaitCommit wedged on failing persistence: %v", err)
	}
	if cp, _ := wrapped.Checkpoint("perst"); cp != 0 {
		t.Fatalf("checkpoint = %d, want 0 (every write failed)", cp)
	}
	if got := eng.Metrics.CheckpointPtr.Load(); got != 0 {
		t.Fatalf("CheckpointPtr = %d, want 0 (never durably written)", got)
	}
}
