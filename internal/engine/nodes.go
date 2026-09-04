package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

// runTransform executes script, split and wasm transforms. Script and wasm
// failures retry per the incoming edge's delivery policy, then dead letter
// (review R6: incoming edge policy governs).
func (e *Engine) runTransform(node *ir.Node) {
	defer e.wg.Done()
	// One wasm invoker per worker goroutine: wazero module instances are not
	// goroutine-safe and die on traps (review-m3 R4). The logger is wrapped
	// so the slow-call watchdog names the node — "which pipeline node is
	// wedged" is the actionable fact (M3-audit B follow-up).
	var invoker *wasmhost.Invoker
	if node.Wasm != nil {
		nodeLogf := func(format string, args ...any) {
			e.Opts.Logf("[node %s] "+format, append([]any{node.Name}, args...)...)
		}
		invoker = node.Wasm.NewInvoker(node.Config.Wasm, nodeLogf, e.Opts.WasmSlowCallWarnMs)
		defer invoker.Close()
	}
	ch := e.chans[node.Name]
	for {
		select {
		case <-e.ctx.Done():
			return
		case inst := <-ch:
			e.processTransform(node, inst, invoker)
		}
	}
}

func (e *Engine) processTransform(node *ir.Node, inst *instance, wasmInvoker *wasmhost.Invoker) {
	if node.Wasm != nil && wasmInvoker != nil {
		e.Metrics.TransformRuns.Add(1)
		retries, backoff := e.deliveryOf(inst.via)
		start := time.Now()

		var werr error
		for attempt := 0; attempt <= retries; attempt++ {
			if attempt > 0 {
				e.Metrics.Retries.Add(1)
				if !e.sleepBackoff(backoff, attempt) {
					// Shutting down mid-retry: leave unsettled for replay.
					return
				}
			}
			var in []byte
			in, werr = json.Marshal(inst.msg.Decoded)
			if werr != nil {
				werr = fmt.Errorf("wasm: encode payload: %w", werr)
				continue
			}
			var out []byte
			out, werr = wasmInvoker.Invoke(e.ctx, in)
			if werr != nil {
				continue
			}
			if len(out) == 0 {
				werr = fmt.Errorf("wasm: transform returned empty output (payload must be JSON)")
				continue
			}
			var decoded any
			if err := json.Unmarshal(out, &decoded); err != nil {
				werr = fmt.Errorf("wasm: output is not valid JSON: %v", err)
				continue
			}
			inst.msg.Decoded = decoded
			werr = nil
			break
		}
		if werr != nil {
			timedOut := strings.Contains(werr.Error(), "exceeded")
			e.Opts.Obs.RecordWasm(e.IR.Config.Name, node.Name, time.Since(start), timedOut)
			e.deadLetter(inst, node.Name, "wasm: "+strings.TrimPrefix(werr.Error(), "wasm: "), "")
			return
		}
		e.Opts.Obs.RecordWasm(e.IR.Config.Name, node.Name, time.Since(start), false)
		e.fanOut(node, inst.seq, inst.msg)
		return
	}

	if node.Script != nil {
		e.Metrics.TransformRuns.Add(1)
		retries, backoff := e.deliveryOf(inst.via)
		scriptStart := time.Now()

		var payloadState, metaState *starhost.MsgState
		var serr *starhost.ScriptError
		for attempt := 0; attempt <= retries; attempt++ {
			if attempt > 0 {
				e.Metrics.Retries.Add(1)
				if !e.sleepBackoff(backoff, attempt) {
					// Shutting down mid-retry: leave unsettled for replay.
					return
				}
			}
			// Fresh binding state per attempt: writes of a failed attempt
			// must not leak into the retry.
			ps := starhost.NewMsgState("payload", inst.msg.Decoded)
			ms := starhost.NewMsgState("meta", inst.msg.Meta)
			serr = node.Script.RunWithParams(ps, ms, e.IR.FrozenConstants, e.IR.FrozenParameters)
			if serr == nil {
				payloadState, metaState = ps, ms
				break
			}
		}
		if serr != nil {
			e.Opts.Obs.RecordScript(e.IR.Config.Name, node.Name, time.Since(scriptStart),
				strings.Contains(serr.Msg, "too many steps"))
			e.deadLetter(inst, node.Name, "script: "+serr.Msg, serr.Backtrace)
			return
		}
		e.Opts.Obs.RecordScript(e.IR.Config.Name, node.Name, time.Since(scriptStart), false)
		if payloadState.Dirty() {
			inst.msg.Decoded = payloadState.GoValue()
		}
		if metaState.Dirty() {
			if m, ok := metaState.MapValue(); ok {
				inst.msg.Meta = m
			}
		}
		e.fanOut(node, inst.seq, inst.msg)
		return
	}

	if node.IsSplit {
		e.processSplit(node, inst)
		return
	}

	// Unreachable (verify rejects), but never drop silently.
	e.deadLetter(inst, node.Name, "transform node has no main field", "")
}

// processSplit turns an array payload into one message per element (review
// R8). Children share the parent's spool identity and message_id; the parent
// settles only when all children's branches settle.
func (e *Engine) processSplit(node *ir.Node, inst *instance) {
	items, ok := inst.msg.Decoded.([]any)
	if !ok {
		e.deadLetter(inst, node.Name, fmt.Sprintf("split: payload is %T, want array", inst.msg.Decoded), "")
		return
	}
	if len(items) == 0 {
		e.settle.done(inst.seq)
		return
	}
	e.settle.add(inst.seq, len(items)-1)
	for _, item := range items {
		child := inst.msg // shallow copy; COW bindings protect payload/meta
		child.Decoded = item
		e.fanOut(node, inst.seq, child)
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

// writeBatch encodes, writes with per-edge delivery retries, and settles or
// dead letters. Mixed-edge batches take the strictest retry policy and
// dead-letter attribution stays per instance (its own via-edge).
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
	timeout := e.Opts.DefaultTimeout
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
			if inst.via.TimeoutMs > 0 {
				timeout = time.Duration(inst.via.TimeoutMs) * time.Millisecond
			}
		}
		batch = append(batch, ready{inst: inst, msg: m})
	}
	if len(batch) == 0 {
		return
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
				return // shutdown: unsettled, replayed later
			}
		}
		writeCtx, cancel := context.WithTimeout(e.ctx, timeout)
		writeStart := time.Now()
		werr := sink.Write(writeCtx, msgs)
		e.Opts.Obs.RecordSinkWrite(e.IR.Config.Name, node.Name, time.Since(writeStart))
		cancel()
		if werr == nil {
			for _, r := range batch {
				e.settle.done(r.inst.seq)
			}
			return
		}
	}
	for _, r := range batch {
		if r.inst.via != nil && !r.inst.via.Required {
			e.Metrics.OptionalDrops.Add(1)
			e.Opts.Obs.RecordOptionalDrop(e.IR.Config.Name, r.inst.via.From+" -> "+node.Name)
			e.settle.done(r.inst.seq)
			continue
		}
		e.deadLetter(r.inst, node.Name, "delivery: sink write failed after retries", "")
	}
}

// deadLetter writes a durable dead letter and settles the branch. A failing
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
			e.settle.done(seq)
			return
		}
		e.Metrics.DlqFailures.Add(1)
		e.Opts.Obs.RecordDlqFailure(e.IR.Config.Name)
		select {
		case <-time.After(e.Opts.DLBackoff):
		case <-e.ctx.Done():
			return // stays unsettled; replayed on restart
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
