package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// runTransform applies one transform node's plugin. Plugins with per-worker
// state implement TransformCloner and get an independent clone per goroutine
// (wasm module instances are not goroutine-safe and die on traps,
// review-m3 R4); stateless plugins (script programs are immutable, split has
// no state) share one Init'ed instance. A failed Clone is worker-fatal: the
// master instance is exactly what the plugin declared unsafe to share, so
// the engine fails the pipeline instead of racing workers on it.
func (e *Engine) runTransform(node *ir.Node) {
	defer e.wg.Done()
	t := e.transforms[node.Name]
	if cloner, ok := t.(registry.TransformCloner); ok {
		clone, err := cloner.Clone()
		if err != nil {
			e.failNode(node.Name, fmt.Errorf("transform worker clone: %w", err))
			return
		}
		t = clone
		defer func() { _ = t.Close() }()
	}
	ch := e.chans[node.Name]
	for {
		select {
		case <-e.ctx.Done():
			return
		case inst := <-ch:
			e.processTransform(node, inst, t)
		}
	}
}

// processTransform applies the plugin to one message: delivery retries per
// the incoming edge's policy (review R6), then dead letter — a transform
// failure never fails the node. Zero outputs filter the message (committed
// as filtered, NoMatch — the same semantics as an edge predicate with no
// matching edge);
// N outputs fan out with the commit accounting expanded for the extra
// branches, the split plugin's 1→N contract.
func (e *Engine) processTransform(node *ir.Node, inst *instance, t registry.Transform) {
	e.Metrics.TransformRuns.Add(1)
	retries, backoff := e.deliveryOf(inst.via)
	start := time.Now()

	var outputs []*registry.Message
	var aerr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			e.Metrics.Retries.Add(1)
			if !e.sleepBackoff(backoff, attempt) {
				// Shutting down mid-retry: leave uncommitted for replay.
				return
			}
		}
		outputs, aerr = t.Apply(&inst.msg)
		if aerr == nil {
			break
		}
	}

	flavor := ""
	if f, ok := t.(registry.TransformFlavor); ok {
		flavor = f.Flavor()
	}
	record := func(flag string) {
		switch flavor {
		case "script":
			e.Opts.Obs.RecordScript(e.IR.Config.Name, node.Name, time.Since(start), flag == "steps")
		case "wasm":
			e.Opts.Obs.RecordWasm(e.IR.Config.Name, node.Name, time.Since(start), flag == "timeout")
		}
	}

	if aerr != nil {
		flag, backtrace := "", ""
		var te *registry.TransformError
		if errors.As(aerr, &te) {
			flag, backtrace = te.Flag, te.Backtrace
		}
		record(flag)
		e.deadLetter(inst, node.Name, node.Config.Plugin+": "+aerr.Error(), backtrace)
		return
	}
	record("")
	if len(outputs) == 0 {
		e.Metrics.NoMatch.Add(1)
		e.Opts.Obs.RecordNoMatch(e.IR.Config.Name, node.Name)
		e.commit.done(inst.seq)
		return
	}
	if len(outputs) > 1 {
		e.commit.add(inst.seq, len(outputs)-1)
	}
	for _, out := range outputs {
		e.fanOut(node, inst.seq, *out)
	}
}

// runSink batches and writes one sink node; batching is engine-owned
// (redesign-v3.md §6.4).
func (e *Engine) runSink(node *ir.Node) {
	defer e.wg.Done()
	ch := e.chans[node.Name]
	sink := e.sinks[node.Name]

	size := 1
	if node.Config.Batch != nil {
		size = node.Config.Batch.Size
	}
	flushEvery := e.Opts.BatchFlush
	if node.Config.Batch != nil && node.Config.Batch.TimeoutMs > 0 {
		flushEvery = time.Duration(node.Config.Batch.TimeoutMs) * time.Millisecond
	}
	var ticker *time.Ticker
	var tickC <-chan time.Time
	if size > 1 {
		ticker = time.NewTicker(flushEvery)
		defer ticker.Stop()
		tickC = ticker.C
	}

	pending := make([]*instance, 0, size)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = make([]*instance, 0, size)
		e.writeBatch(node, sink, batch)
	}
	for {
		select {
		case <-e.ctx.Done():
			flush()
			return
		case <-tickC:
			flush()
		case inst := <-ch:
			pending = append(pending, inst)
			if len(pending) >= size {
				flush()
			}
		}
	}
}

// writeBatch encodes, writes with per-edge delivery retries, and commits or
// dead letters. Mixed-edge batches take the strictest rule per dimension
// (review-2026-09): the max retry count, and the max explicit TimeoutMs —
// DefaultTimeout applies only when no edge in the batch sets one, so a short
// edge timeout can never truncate a longer write sharing the batch. Dead-letter
// attribution stays per instance (its own via-edge).
func (e *Engine) writeBatch(node *ir.Node, sink registry.Sink, insts []*instance) {
	encoderName := node.Config.Encoder
	if encoderName == "" {
		encoderName = "json"
	}
	encoder, err := e.codec(encoderName, e.Reg)
	if err != nil {
		for _, inst := range insts {
			e.deadLetter(inst, node.Name, "encoder: "+err.Error(), "")
		}
		return
	}

	type ready struct {
		inst *instance
		msg  registry.Message
	}
	var batch []ready
	retries, backoff := 0, "exponential"
	timeoutMs := 0
	for _, inst := range insts {
		m := inst.msg
		if m.Decoded != nil {
			out, encErr := encoder.Encode(m.Decoded)
			if encErr != nil {
				e.deadLetter(inst, node.Name, "encode: "+encErr.Error(), "")
				continue
			}
			m.Out = out
		}
		if node.OrderKey != nil {
			if key, kerr := node.OrderKey.EvalString(m.Decoded, m.Meta); kerr == nil {
				m.Key = []byte(key)
			}
		}
		if inst.via != nil {
			if inst.via.Retries > retries {
				retries = inst.via.Retries
			}
			if inst.via.TimeoutMs > timeoutMs {
				timeoutMs = inst.via.TimeoutMs
			}
		}
		batch = append(batch, ready{inst: inst, msg: m})
	}
	if len(batch) == 0 {
		return
	}
	timeout := e.Opts.DefaultTimeout
	if timeoutMs > 0 {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}

	msgs := make([]registry.Message, len(batch))
	for i, r := range batch {
		msgs[i] = r.msg
	}
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			e.Metrics.Retries.Add(1)
			e.Opts.Obs.RecordRetry(e.IR.Config.Name, node.Name)
			if !e.sleepBackoff(backoff, attempt) {
				return // shutdown: uncommitted, replayed later
			}
		}
		writeCtx, cancel := context.WithTimeout(e.ctx, timeout)
		writeStart := time.Now()
		werr := sink.Write(writeCtx, msgs)
		e.Opts.Obs.RecordSinkWrite(e.IR.Config.Name, node.Name, time.Since(writeStart))
		cancel()
		if werr == nil {
			for _, r := range batch {
				e.commit.done(r.inst.seq)
			}
			return
		}
	}
	for _, r := range batch {
		if r.inst.via != nil && !r.inst.via.Required {
			e.Metrics.OptionalDrops.Add(1)
			e.Opts.Obs.RecordOptionalDrop(e.IR.Config.Name, r.inst.via.From+" -> "+node.Name)
			e.commit.done(r.inst.seq)
			continue
		}
		e.deadLetter(r.inst, node.Name, "delivery: sink write failed after retries", "")
	}
}

// deadLetter writes a durable dead letter and commits the branch. A failing
// dead letter write NEVER drops the message: it retries until it succeeds or
// the engine shuts down (invariant 4: degraded, not lossy).
func (e *Engine) deadLetter(inst *instance, node, reason, backtrace string) {
	e.deadLetterMsg(inst.seq, inst.msg, node, edgeLabel(inst.via), reason, backtrace)
}

func (e *Engine) deadLetterMsg(seq int64, msg registry.Message, node, edge, reason, backtrace string) {
	runID := ""
	if e.Opts.MetaStamps != nil {
		if v, ok := e.Opts.MetaStamps["job_run_id"].(string); ok {
			runID = v
		}
	}
	if runID == "" {
		if m, ok := msg.Meta["job_run_id"].(string); ok {
			runID = m // replayed message keeps its original run attribution
		}
	}
	dl := store.DeadLetter{
		Pipeline:  e.IR.Config.Name,
		MessageID: msg.ID,
		RunID:     runID,
		Node:      node,
		Edge:      edge,
		Reason:    reason,
		Backtrace: backtrace,
		Raw:       msg.Raw,
		Codec:     msg.Codec,
		Meta:      msg.Meta,
		Cursor:    msg.Cursor,
		SrcName:   msg.SrcName,
		SrcSeq:    msg.SrcSeq,
	}
	for {
		err := e.Store.WriteDeadLetter(dl)
		if err == nil {
			e.Metrics.DeadLettered.Add(1)
			e.Opts.Obs.RecordDeadLetter(e.IR.Config.Name, node, obs.ReasonClass(reason))
			e.finishSpan(seq, "dead_letter", reason)
			e.commit.done(seq)
			return
		}
		e.Metrics.DlqFailures.Add(1)
		e.Opts.Obs.RecordDlqFailure(e.IR.Config.Name)
		select {
		case <-time.After(e.Opts.DLBackoff):
		case <-e.ctx.Done():
			return // stays uncommitted; replayed on restart
		}
	}
}

func (e *Engine) deliveryOf(edge *ir.Edge) (retries int, backoff string) {
	if edge == nil {
		return 0, "exponential"
	}
	return edge.Retries, edge.Backoff
}

func (e *Engine) sleepBackoff(backoff string, attempt int) bool {
	d := e.Opts.BackoffBase
	if backoff == "constant" {
		// base
	} else {
		d <<= min(attempt-1, 16)
		if d > 30*time.Second {
			d = 30 * time.Second
		}
	}
	select {
	case <-time.After(d):
		return true
	case <-e.ctx.Done():
		return false
	}
}

func edgeLabel(edge *ir.Edge) string {
	if edge == nil {
		return ""
	}
	return edge.From + " -> " + edge.To
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
