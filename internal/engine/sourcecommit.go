package engine

import (
	"context"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// sourceCommitter is the per-source worker that receives coalesced frontier
// advances, calls Source.Commit with the maximum and persists the returned
// state (CONTEXT.md "Source committer"). It decouples watermark persistence
// from the committing goroutine, so a source may hold an internal lock across
// emit without deadlocking the pipeline (candidate 01: the file source does
// exactly that, deliberately).
//
// Coalescing: post keeps the highest unpersisted frontier and wakes the
// worker at most once per value. A failed Commit or state write keeps the
// pending value and is retried on the next advance — or by the shutdown
// flush, which Run runs synchronously after drain with a bounded,
// non-cancelled context (the engine ctx is already cancelled; a cancelled
// Commit would discard the last frontier).
//
// The monotonic guard lives here (the engine's old srcPersisted): a late
// out-of-order advance can never regress the persisted state, and a frontier
// at or below the persisted one is a no-op.
type sourceCommitter struct {
	pipeline string
	node     string
	src      registry.Source
	store    store.SpoolStore
	timeout  time.Duration

	mu        sync.Mutex
	pending   int64 // highest frontier posted, not yet persisted
	persisted int64 // highest frontier whose state was persisted

	wake chan struct{}
	done chan struct{} // closed when the worker goroutine exits
}

func newSourceCommitter(pipeline, node string, src registry.Source, st store.SpoolStore, timeout time.Duration) *sourceCommitter {
	return &sourceCommitter{
		pipeline: pipeline,
		node:     node,
		src:      src,
		store:    st,
		timeout:  timeout,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// post records one frontier advance (coalesced maximum) and wakes the worker
// when there is unpersisted work. Non-blocking: it runs on the committing
// goroutine, outside the tracker lock.
func (c *sourceCommitter) post(frontier int64) {
	if frontier <= 0 {
		return
	}
	c.mu.Lock()
	if frontier > c.pending {
		c.pending = frontier
	}
	need := c.pending > c.persisted
	c.mu.Unlock()
	if !need {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// run is the committer goroutine; it exits when the engine context is
// cancelled (Run then flushes whatever is still pending).
func (c *sourceCommitter) run(ctx context.Context) {
	defer close(c.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
			c.commit(ctx)
		}
	}
}

// wait blocks until the worker has exited, or the bound elapses (a source
// wedged in Commit ignores the cancelled engine ctx; hanging Run on it would
// trade a replay window for a stuck shutdown — the pending frontier is only a
// duplicate-delivery question).
func (c *sourceCommitter) wait(bound time.Duration) {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-c.done:
	case <-timer.C:
	}
}

// commit attempts one pending frontier. A failed commit or state write keeps
// the pending value: the next advance (or the shutdown flush) retries it.
func (c *sourceCommitter) commit(ctx context.Context) {
	c.mu.Lock()
	frontier, persisted := c.pending, c.persisted
	c.mu.Unlock()
	if frontier <= persisted {
		return
	}
	state, err := c.src.Commit(ctx, frontier)
	if err != nil || state == nil {
		return
	}
	if err := c.store.SetSourceState(c.pipeline, c.node, state, frontier); err != nil {
		return
	}
	c.mu.Lock()
	if frontier > c.persisted {
		c.persisted = frontier
	}
	c.mu.Unlock()
}

// flush is the shutdown path: one final synchronous attempt with a bounded,
// non-cancelled context, so a clean stop leaves the source state at the
// latest frontier even when the async worker never got to it. It only runs
// after the worker has exited (Run waits first): a still-in-flight commit
// must not race a second Source.Commit call.
func (c *sourceCommitter) flush() {
	select {
	case <-c.done:
	default:
		return // still in flight after the drain bound: it persists its own pending value
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	c.commit(ctx)
}
