package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/registry"
)

// admission is the engine's inbound path: one entry (`admit`) turns one
// inbound message into a spooled, commit-registered, dispatched message
// (CONTEXT.md "Admission"). It owns the backpressure gate, the acquired-slot
// ledger, the admitting counter, stamping, spool append (or skip), commit
// registration and the dispatch step.
//
// The three modes differ only in request fields:
//
//   - live — a source emission: stamped (message_id, ingest_time, source,
//     MetaStamps), appended, its source-watermark reference registered
//     (commit.arrived(seq, sourceNode, srcSeq)), an accept time and a
//     per-message span, and dispatched out of the source node. The codec is
//     the node's decoder (json default).
//   - replay — a crash-recovery row: already spooled (the existing seq, no
//     append), original stamps kept, not attributable to a live emission
//     (commit.arrived(seq, "", 0)), no accept time or span. The codec is the
//     message's spooled codec (json default).
//   - inject — an operator/test message: appended, stamped (message_id,
//     ingest_time, source; plus injected_at when it enters an internal node),
//     not attributable to a live emission, no accept time or span. The codec
//     is the message's codec (json default); a caller that injects at a
//     source without a codec of its own passes the node's decoder (testrun).
//     Dispatch goes INTO the entry node — except at a source entry, which has
//     no inbound processing and therefore fans out, exactly like live.
//
// Every mode takes admission quota (one slot per uncommitted message): the
// uncommitted set is ≤ HighWatermark by construction, so replay cannot wedge
// on the gate, and the quota is uniform across entries.
type admission struct {
	eng *Engine

	// gate is the admission semaphore. The jobs manager may hand in a SHARED
	// pool (Options.Admission) so limits.max_in_flight aggregates across
	// concurrent overlap:all runs instead of multiplying per run.
	gate chan struct{}

	// admitting counts callers waiting on the gate: WaitCommit must not read
	// "committed" while an admission is in flight (a gate-blocked message
	// momentarily looks like outstanding == 0 — flaky-test class, M3 CI).
	admitting atomic.Int64

	// acquired is the ledger of spool seqs holding an admission slot; release
	// is idempotent so a commit and a force-terminate cannot double-release.
	acquired   map[int64]bool
	acquiredMu sync.Mutex

	// acceptedAt records the accept time per spool seq (commit latency),
	// live mode only.
	acceptedAt map[int64]time.Time
	acceptMu   sync.Mutex
}

// admitMode selects the admission variant; the differences are request
// fields, not code paths.
type admitMode int

const (
	admitLive admitMode = iota
	admitReplay
	admitInject
)

// admitRequest is one inbound message plus its mode differences.
type admitRequest struct {
	mode   admitMode
	node   string // entry node: the source (live) or the injection target
	msg    registry.Message
	seq    int64     // replay: the existing spool sequence (no append)
	ingest time.Time // live/inject: the accept clock (replay keeps the spooled stamps)
	// intoNode delivers INTO the entry node instead of fanning out of it
	// (internal injection). Source entries always fan out.
	intoNode bool
}

func newAdmission(e *Engine) *admission {
	a := &admission{
		eng:        e,
		acquired:   map[int64]bool{},
		acceptedAt: map[int64]time.Time{},
	}
	if e.Opts.Admission != nil {
		a.gate = e.Opts.Admission // shared pool: pipeline-aggregated quota
	} else {
		a.gate = make(chan struct{}, e.Opts.HighWatermark)
	}
	return a
}

// admit runs one inbound message through the gate, stamping, the spool and
// the dispatch. The returned error is the admission verdict: nil = durably
// accepted; a ctx error = engine shutdown; anything else = refusal (not
// durable, never visible, safe to re-emit) — the emit contract of
// registry.Source.
func (a *admission) admit(ctx context.Context, req admitRequest) (int64, error) {
	e := a.eng

	// Backpressure: block while too many uncommitted messages are in flight.
	a.admitting.Add(1)
	select {
	case a.gate <- struct{}{}:
		a.admitting.Add(-1)
	case <-ctx.Done():
		a.admitting.Add(-1)
		e.Metrics.Backpressured.Add(1)
		e.Opts.Obs.RecordBackpressure(e.IR.Config.Name, req.node)
		return 0, ctx.Err()
	}

	msg := req.msg
	seq := req.seq
	ingest := req.ingest
	if req.mode == admitLive || req.mode == admitInject {
		// Stamping (live and inject). Replay keeps the spooled stamps.
		if msg.ID == "" {
			msg.ID = e.Opts.NewID()
		}
		msg.SrcName = req.node
		if ingest.IsZero() {
			ingest = e.Opts.Clock()
		}
		meta := cloneMeta(msg.Meta)
		meta["message_id"] = msg.ID
		meta["ingest_time"] = ingest.UTC().Format(time.RFC3339Nano)
		meta["source"] = req.node
		if req.mode == admitInject && req.intoNode {
			// Marks the row as entering an internal node: crash replay reads
			// it back and re-enters INTO that node instead of fanning out of
			// a source that never produced it.
			meta["injected_at"] = req.node
		}
		for k, v := range e.Opts.MetaStamps {
			if _, exists := meta[k]; !exists {
				meta[k] = v
			}
		}
		msg.Meta = meta
		if req.mode == admitLive {
			codecName := e.IR.Nodes[req.node].Config.Decoder
			if codecName == "" {
				codecName = "json"
			}
			msg.Codec = codecName
		} else if msg.Codec == "" {
			msg.Codec = "json"
		}

		var err error
		seq, err = e.Store.AppendSpool(e.IR.Config.Name, msg, ingest)
		if err != nil {
			<-a.gate
			e.Metrics.SpoolFailures.Add(1)
			e.Opts.Obs.RecordSpoolFailure(e.IR.Config.Name)
			return 0, fmt.Errorf("engine: spool append failed; message NOT delivered: %w", err)
		}
		if req.mode == admitLive {
			e.Metrics.MessagesIn.Add(1)
			e.Opts.Obs.RecordMessageIn(e.IR.Config.Name, req.node)
		}
	} else if msg.Codec == "" {
		// Replay: the spooled codec, json default.
		msg.Codec = "json"
	}

	a.acquire(seq)
	if req.mode == admitLive {
		a.acceptMu.Lock()
		a.acceptedAt[seq] = ingest
		a.acceptMu.Unlock()
		e.startMessageSpan(seq, msg.ID, req.node)
		// The source-watermark reference: only a live emission is
		// attributable to this run's source counter.
		e.commit.arrived(seq, req.node, req.msg.SrcSeq)
	} else {
		e.commit.arrived(seq, "", 0)
	}

	if req.intoNode {
		e.dispatchInternal(req.node, seq, msg)
	} else {
		e.dispatchFrom(req.node, seq, msg)
	}
	return seq, nil
}

// acquire records the admission slot for one spooled seq.
func (a *admission) acquire(seq int64) {
	a.acquiredMu.Lock()
	a.acquired[seq] = true
	a.acquiredMu.Unlock()
}

// release frees the backpressure slot of one terminal message (committed,
// dead-lettered or abandoned). It is idempotent: a seq whose slot a racing
// path already returned (a commit sweep and Abandon, or a force-terminate
// after a normal commit) is skipped, never double-released.
func (a *admission) release(seq int64) {
	a.acquiredMu.Lock()
	if !a.acquired[seq] {
		a.acquiredMu.Unlock()
		return
	}
	delete(a.acquired, seq)
	a.acquiredMu.Unlock()
	<-a.gate
}

// takeAccepted removes and returns one seq's accept time (zero when absent).
func (a *admission) takeAccepted(seq int64) time.Time {
	a.acceptMu.Lock()
	accepted := a.acceptedAt[seq]
	delete(a.acceptedAt, seq)
	a.acceptMu.Unlock()
	return accepted
}

// replayEntry resolves where a spooled row re-enters the DAG: the injection
// target when the row was injected (injected_at wins over source —
// review-2026-09; fanning OUT of a transform would skip its script and
// deliver raw to downstream, and a sink has no out-edges so the row would be
// dropped as NoMatch), else its originating source node. Unknown node names
// return ok=false (the row is spooled but no longer dispatchable).
func (e *Engine) replayEntry(msg registry.Message) (node string, intoNode, ok bool) {
	if injected, _ := msg.Meta["injected_at"].(string); injected != "" {
		if _, known := e.IR.Nodes[injected]; known {
			return injected, true, true
		}
	}
	if src, _ := msg.Meta["source"].(string); src != "" {
		if _, known := e.IR.Nodes[src]; known {
			return src, false, true
		}
	}
	return "", false, false
}

// injectAt is the shared shell of InjectAt/InjectReplay: it resolves the
// entry node's shape and runs one inject admission.
func (e *Engine) injectAt(node string, msg registry.Message) (int64, error) {
	n, ok := e.IR.Nodes[node]
	if !ok {
		return 0, fmt.Errorf("engine: unknown node %q", node)
	}
	if !e.started.Load() {
		return 0, fmt.Errorf("engine: not started; call Run first")
	}
	// A source entry has no inbound processing: the message fans out of it,
	// exactly like a live emission. Every other entry is delivered INTO the
	// node (transform script, sink batch) — the operator-replay semantics.
	intoNode := n.Section != config.SectionSource
	return e.admit.admit(e.ctx, admitRequest{
		mode:     admitInject,
		node:     node,
		msg:      msg,
		intoNode: intoNode,
	})
}
