package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/observability"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
	"github.com/edgesets/edgestream/internal/topology"
	"github.com/google/uuid"
)

type Pipeline struct {
	ir             *topology.TopologyIR
	reg            *registry.Registry
	metrics        *observability.Metrics
	stages         map[string]stage.Stage
	stageErrorMode map[string]string
	inflight       atomic.Int32
	graph          *runtimeGraph
	decoders       map[string]codec.Codec // stage ID → decoder
	encoders       map[string]codec.Codec // stage ID → encoder
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	stageWG        sync.WaitGroup
	started        atomic.Bool
}

func NewPipeline(ctx context.Context, reg *registry.Registry, ir *topology.TopologyIR, metrics *observability.Metrics) (*Pipeline, error) {
	p := &Pipeline{
		ir:             ir,
		reg:            reg,
		metrics:        metrics,
		stages:         make(map[string]stage.Stage),
		stageErrorMode: make(map[string]string),
		decoders:       make(map[string]codec.Codec),
		encoders:       make(map[string]codec.Codec),
	}
	for _, st := range ir.Stages {
		if st.ErrorMode != "" {
			p.stageErrorMode[st.ID] = st.ErrorMode
		}
	}
	for _, st := range ir.Stages {
		if err := p.instantiateStage(st); err != nil {
			return nil, err
		}
	}
	if err := p.resolveCodecs(ir); err != nil {
		return nil, err
	}
	g, err := buildRuntimeGraph(ir)
	if err != nil {
		return nil, err
	}
	p.graph = g
	return p, nil
}

func (p *Pipeline) resolveCodecs(ir *topology.TopologyIR) error {
	for _, st := range ir.Stages {
		if dec, err := p.resolveCodecRef(st.Decoder, "decoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		} else if dec != nil {
			p.decoders[st.ID] = dec
		}
		if enc, err := p.resolveCodecRef(st.Encoder, "encoder"); err != nil {
			return fmt.Errorf("stage %q: %w", st.ID, err)
		} else if enc != nil {
			p.encoders[st.ID] = enc
		}
	}
	return nil
}

func (p *Pipeline) resolveCodecRef(ref *config.CodecRef, role string) (codec.Codec, error) {
	if ref == nil || ref.IsEmpty() {
		return nil, nil
	}
	if ref.Ref != "" {
		cir, ok := p.ir.Codecs[ref.Ref]
		if !ok {
			return nil, fmt.Errorf("%s ref %q not found", role, ref.Ref)
		}
		cfg := cir.Config
		if ref.Config != nil {
			merged := make(map[string]any, len(cfg)+len(ref.Config))
			for k, v := range cfg {
				merged[k] = v
			}
			for k, v := range ref.Config {
				merged[k] = v
			}
			cfg = merged
		}
		return p.reg.CreateCodec(cir.Type, cfg)
	}
	if ref.Type != "" {
		return p.reg.CreateCodec(ref.Type, ref.Config)
	}
	return nil, nil
}

func (p *Pipeline) instantiateStage(st topology.StageIR) error {
	cfg := map[string]any{}
	if st.Config != nil {
		for k, v := range st.Config {
			cfg[k] = v
		}
	}
	cfg["__decoder"] = st.Decoder
	cfg["__encoder"] = st.Encoder
	cfg["__predicate"] = st.Predicate
	cfg["__workers"] = st.Workers
	cfg["__batch"] = st.Batch
	cfg["__ordering"] = st.Ordering
	cfg["__max_in_flight"] = st.MaxInFlight

	var s stage.Stage
	var err error
	switch st.Kind {
	case topology.KindSource:
		var src stage.Source
		src, err = p.reg.CreateSource(st.Type, st.ID, cfg)
		s = src
	case topology.KindTransform:
		var tr stage.Transform
		tr, err = p.reg.CreateTransform(st.Type, st.ID, cfg)
		s = tr
	case topology.KindSink:
		var sk stage.Sink
		sk, err = p.reg.CreateSink(st.Type, st.ID, cfg)
		s = sk
	default:
		return fmt.Errorf("unknown stage kind %q", st.Kind)
	}
	if err != nil {
		return fmt.Errorf("stage %q: %w", st.ID, err)
	}
	p.stages[st.ID] = s
	return nil
}

func (p *Pipeline) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return fmt.Errorf("pipeline %q already started", p.ir.Name)
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for id, st := range p.stages {
		if err := st.Init(runCtx); err != nil {
			cancel()
			return fmt.Errorf("init stage %q: %w", id, err)
		}
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(runCtx)
	}()
	return nil
}

func (p *Pipeline) Stop(ctx context.Context) error {
	// 1. Stop all sources first — no new messages
	for id, st := range p.stages {
		if _, ok := st.(stage.Source); ok {
			_ = st.Stop(ctx)
		}
		_ = id
	}
	// 2. Cancel context to signal all goroutines to drain
	if p.cancel != nil {
		p.cancel()
	}
	// 3. Wait for in-flight messages to drain (with timeout from context)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		p.stageWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	// 4. Close edge buffers (fsync disk WAL)
	for _, eb := range p.graph.allEdgeInbounds() {
		_ = eb.Close()
	}
	// 5. Flush all sinks (write remaining batches)
	for _, st := range p.stages {
		if sk, ok := st.(stage.Sink); ok {
			_ = sk.Flush(ctx)
		}
	}
	// 6. Stop all remaining stages (transforms + sinks)
	for _, st := range p.stages {
		if _, ok := st.(stage.Source); ok {
			continue // sources already stopped
		}
		_ = st.Stop(ctx)
	}
	return nil
}

func (p *Pipeline) startTransformFanIn(ctx context.Context, node *runtimeNode) {
	for _, eb := range node.inboundEdges {
		eb := eb
		p.stageWG.Add(1)
		go func() {
			defer p.stageWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-eb.Out():
					if !ok {
						return
					}
					batch := []*message.Message{msg}
					select {
					case <-ctx.Done():
						msg.Ack(ctx.Err())
						return
					case node.batchIn <- batch:
					}
				}
			}
		}()
	}
}

func (p *Pipeline) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			_ = r
		}
	}()

	for _, eb := range p.graph.allEdgeInbounds() {
		eb.Start(ctx)
	}

	for id, node := range p.graph.nodes {
		if node.kind == topology.KindTransform {
			p.startTransformFanIn(ctx, node)
		}
		_ = id
	}

	for id, node := range p.graph.nodes {
		if node.kind != topology.KindSink {
			continue
		}
		p.stageWG.Add(1)
		go func(sinkID string, n *runtimeNode) {
			defer p.stageWG.Done()
			p.runSink(ctx, sinkID, n)
		}(id, node)
	}

	var transformNodes []*runtimeNode
	for id, node := range p.graph.nodes {
		if node.kind != topology.KindTransform {
			continue
		}
		transformNodes = append(transformNodes, node)
		_ = id
	}
	workerAlloc := allocateTransformWorkers(transformNodes, p.ir.Engine.MaxWorkers)
	for id, node := range p.graph.nodes {
		if node.kind != topology.KindTransform {
			continue
		}
		workers := workerAlloc[id]
		if workers < 1 {
			continue
		}
		for i := 0; i < workers; i++ {
			p.stageWG.Add(1)
			go func(trID string, n *runtimeNode) {
				defer p.stageWG.Done()
				p.runTransform(ctx, trID, n)
			}(id, node)
		}
	}

	for id, node := range p.graph.nodes {
		if node.kind != topology.KindSource {
			continue
		}
		p.stageWG.Add(1)
		go func(srcID string, n *runtimeNode) {
			defer p.stageWG.Done()
			p.runSource(ctx, srcID, n)
		}(id, node)
	}

	<-ctx.Done()
	p.stageWG.Wait()
}

func (p *Pipeline) runSource(ctx context.Context, id string, node *runtimeNode) {
	src := p.stages[id].(stage.Source)
	out := make(chan *message.Message, node.outBuffer)
	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Consume(ctx, out)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				_ = err
			}
			// Consume already returned, so nothing more is sent to out — but
			// messages it already produced may still be buffered there. The
			// select above can pick errCh while out is also ready; drain the
			// remainder instead of stranding it.
			p.drainSourceOut(ctx, id, src, out)
			return
		case msg, ok := <-out:
			if !ok {
				return
			}
			if !p.handleSourceMessage(ctx, id, src, msg) {
				return
			}
		}
	}
}

// drainSourceOut flushes messages a source handed to out before Consume
// returned. Consume has exited, so a non-blocking read sees everything left.
func (p *Pipeline) drainSourceOut(ctx context.Context, id string, src stage.Source, out chan *message.Message) {
	for {
		select {
		case msg, ok := <-out:
			if !ok {
				return
			}
			if !p.handleSourceMessage(ctx, id, src, msg) {
				return
			}
		default:
			return
		}
	}
}

// handleSourceMessage runs the normal per-message source pipeline. It returns
// false when the lifecycle could not be started (shutdown backpressure), in
// which case the message is nacked so sources see a terminal ack.
func (p *Pipeline) handleSourceMessage(ctx context.Context, id string, src stage.Source, msg *message.Message) bool {
	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}
	msg.SetSourceStageID(id)
	if dec, ok := p.decoders[id]; ok {
		if msg.ParsedCodec() == "" {
			msg.SetParsedCodec(dec.Name())
		}
		if msg.DecoderStageID() == "" {
			msg.SetDecoderStageID(id)
		}
	}
	if acking, ok := src.(stage.AckingSource); ok {
		// Wrap, not Set: an ackFn attached earlier (by the source
		// itself or a buffer layer) must stay on the chain.
		msg.WrapAckFn(func(err error) {
			acking.OnAck(msg, err)
		})
	}
	if err := p.beginMessageLifecycle(ctx, msg); err != nil {
		msg.Ack(err)
		return false
	}
	p.dispatchFrom(ctx, id, msg)
	return true
}

func (p *Pipeline) ensureParsed(msg *message.Message) error {
	if msg.ParsedData() != nil {
		return nil
	}
	stageID := msg.DecoderStageID()
	if stageID == "" {
		return nil
	}
	dec, ok := p.decoders[stageID]
	if !ok {
		return nil
	}
	data, err := dec.Decode(msg.Payload)
	if err != nil {
		return fmt.Errorf("codec %q decode: %w", dec.Name(), err)
	}
	msg.SetParsedData(data)
	return nil
}

func (p *Pipeline) reserializeIfDirty(msg *message.Message, encoderID string) error {
	if !msg.ParsedDirty() {
		return nil
	}
	enc, ok := p.encoders[encoderID]
	if !ok {
		return nil // no encoder — keep payload as-is
	}
	data := msg.ParsedData()
	if data == nil {
		return nil
	}
	newPayload, err := enc.Encode(data)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	msg.BackupOriginalPayload()
	msg.Payload = newPayload
	return nil
}

func (p *Pipeline) dispatchFrom(ctx context.Context, fromID string, msg *message.Message) {
	edges := p.graph.outgoing[fromID]
	if len(edges) == 0 {
		msg.Ack(nil)
		return
	}
	matched := p.matchEdges(ctx, fromID, edges, msg)
	if len(matched) == 0 {
		msg.Ack(nil)
		return
	}
	var pending int32 = int32(len(matched))
	var firstErr atomic.Value // stores the first non-nil error
	var errStored int32       // 0 = no error stored yet

	for _, edge := range matched {
		child := msg.ShallowCopy()
		child.SetAckFn(func(err error) {
			if err != nil && atomic.CompareAndSwapInt32(&errStored, 0, 1) {
				firstErr.Store(err)
			}
			if atomic.AddInt32(&pending, -1) == 0 {
				var ackErr error
				if stored := firstErr.Load(); stored != nil {
					ackErr = stored.(error)
				}
				msg.Ack(ackErr)
			}
		})
		p.sendToInbound(ctx, edge, child)
	}
}

func (p *Pipeline) matchEdges(ctx context.Context, fromID string, edges []topology.EdgeIR, msg *message.Message) []topology.EdgeIR {
	node := p.graph.nodes[fromID]
	var matched []topology.EdgeIR
	for _, edge := range edges {
		if edge.Condition == "" {
			matched = append(matched, edge)
			continue
		}
		prg := node.conditions[edge.To]
		ok, err := p.evalCondition(ctx, prg, msg)
		if err != nil {
			treatAsFalse, ackErr := p.handleEvalError(fromID, err)
			if treatAsFalse {
				continue
			}
			if edge.Required {
				msg.Ack(ackErr)
				return nil
			}
			continue
		}
		if ok {
			matched = append(matched, edge)
		}
	}
	return matched
}

func (p *Pipeline) runTransform(ctx context.Context, id string, node *runtimeNode) {
	tr := p.stages[id].(stage.Transform)
	for {
		select {
		case <-ctx.Done():
			// Nack batches still queued so upstream sources are not left
			// hanging after shutdown.
			for {
				select {
				case batch := <-node.batchIn:
					p.ackBatchError(id, batch, ctx.Err())
				default:
					return
				}
			}
		case batch := <-node.batchIn:
			var filtered []*message.Message
			var passThrough []*message.Message
			for _, m := range batch {
				if err := p.ensureParsed(m); err != nil {
					p.ackMessageError(id, m, err)
					continue
				}
				// Check predicate — false means skip this transform (pass-through)
				if node.predicate != nil {
					ok, evalErr := p.evalCondition(ctx, node.predicate, m)
					if evalErr != nil {
						p.ackMessageError(id, m, evalErr)
						continue
					}
					if !ok {
						passThrough = append(passThrough, m)
						continue
					}
				}
				filtered = append(filtered, m)
			}
			// Pass-through messages skip Process and go directly to downstream
			for _, m := range passThrough {
				p.dispatchFrom(ctx, id, m)
			}
			if len(filtered) == 0 {
				continue
			}
			start := time.Now()
			out, err := tr.Process(ctx, filtered)
			if p.metrics != nil {
				p.metrics.ObserveStage(p.ir.Name, id, topology.KindTransform, time.Since(start))
			}
			if err != nil {
				if p.metrics != nil {
					p.metrics.IncStageError(p.ir.Name, id, "process")
				}
				p.ackBatchError(id, filtered, err)
				continue
			}
			p.dispatchTransformOutputs(ctx, id, filtered, out)
		}
	}
}

func (p *Pipeline) runSink(ctx context.Context, id string, node *runtimeNode) {
	sk := p.stages[id].(stage.Sink)
	batchSize := 1
	var batchTimeout time.Duration
	maxInFlight := 1
	for _, st := range p.ir.Stages {
		if st.ID != id {
			continue
		}
		if st.Batch != nil {
			if st.Batch.Size > 0 {
				batchSize = st.Batch.Size
			}
			if st.Batch.Timeout != "" {
				batchTimeout, _ = time.ParseDuration(st.Batch.Timeout)
			}
		}
		if st.MaxInFlight > 0 {
			maxInFlight = st.MaxInFlight
		}
		if st.Ordering == "ordered" {
			maxInFlight = 1
		}
	}
	delivery := p.findDeliveryForStage(id)

	ingress := make(chan *message.Message, node.outBuffer)
	for _, eb := range node.inboundEdges {
		eb := eb
		p.stageWG.Add(1)
		go func() {
			defer p.stageWG.Done()
			for msg := range eb.Out() {
				select {
				case <-ctx.Done():
					msg.Ack(ctx.Err())
					return
				case ingress <- msg:
				}
			}
		}()
	}

	type writeJob struct {
		batch []*message.Message
	}
	writeCh := make(chan writeJob, maxInFlight)

	// Once the pipeline context is cancelled, in-flight writes switch to an
	// independent drain context (engine.drain_timeout) so the final batches
	// still get a live deadline instead of failing immediately.
	var drainCtx atomic.Value // stores *drainWriteCtx
	writeCtx := func() context.Context {
		if ctx.Err() == nil {
			return ctx
		}
		if d := drainCtx.Load(); d != nil {
			return d.(*drainWriteCtx).ctx
		}
		d := p.newDrainWriteCtx()
		drainCtx.Store(d)
		return d.ctx
	}

	var writerWG sync.WaitGroup
	for i := 0; i < maxInFlight; i++ {
		p.stageWG.Add(1)
		writerWG.Add(1)
		go func() {
			defer p.stageWG.Done()
			defer writerWG.Done()
			// Drain writeCh until it is closed: jobs queued at shutdown must
			// still be written and acked, not abandoned.
			for job := range writeCh {
				p.flushSinkBatch(writeCtx(), sk, id, delivery, job.batch)
			}
		}()
	}

	batch := make([]*message.Message, 0, batchSize)
	var timer *time.Timer
	var timerC <-chan time.Time
	if batchTimeout > 0 {
		timer = time.NewTimer(batchTimeout)
		timerC = timer.C
	}
	enqueue := func(toFlush []*message.Message) {
		if len(toFlush) == 0 {
			return
		}
		cp := make([]*message.Message, len(toFlush))
		copy(cp, toFlush)
		select {
		case <-ctx.Done():
			p.flushSinkBatch(writeCtx(), sk, id, delivery, cp)
		case writeCh <- writeJob{batch: cp}:
		}
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		enqueue(batch)
		batch = batch[:0]
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(batchTimeout)
		}
	}
	for {
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			flush()
			close(writeCh)
			// Wait for writers to finish the queued jobs, then release the
			// drain context if one was created.
			writerWG.Wait()
			if d := drainCtx.Load(); d != nil {
				d.(*drainWriteCtx).cancel()
			}
			return
		case <-timerC:
			flush()
		case msg := <-ingress:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
}

// drainWriteCtx bounds writes that outlive the cancelled pipeline context.
type drainWriteCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// newDrainWriteCtx returns a fresh context bounded by engine.drain_timeout
// (default 30s) for writes that outlive the cancelled pipeline context.
func (p *Pipeline) newDrainWriteCtx() *drainWriteCtx {
	d := 30 * time.Second
	if p.ir != nil && p.ir.Engine.DrainTimeout != "" {
		if parsed, err := time.ParseDuration(p.ir.Engine.DrainTimeout); err == nil && parsed > 0 {
			d = parsed
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	return &drainWriteCtx{ctx: ctx, cancel: cancel}
}

func (p *Pipeline) flushSinkBatch(ctx context.Context, sk stage.Sink, id string, delivery *config.DeliverySpec, batch []*message.Message) {
	start := time.Now()
	for _, m := range batch {
		_ = p.reserializeIfDirty(m, id)
	}
	retries, err := p.writeWithRetry(ctx, sk, batch, delivery)
	var dlqDelivered []bool
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncStageError(p.ir.Name, id, "write")
		}
		dlqDelivered = p.deliverToDLQ(batch, err, id, delivery, retries)
	}
	if p.metrics != nil {
		p.metrics.ObserveStage(p.ir.Name, id, topology.KindSink, time.Since(start))
	}
	for i, m := range batch {
		ackErr := err
		if dlqDelivered != nil && dlqDelivered[i] {
			// The failure reached its terminal disposition (DLQ): ack with
			// nil so the source commits instead of redelivering — Kafka
			// Connect DLQ semantics.
			ackErr = nil
		}
		m.Ack(ackErr)
	}
}

func (p *Pipeline) findDeliveryForStage(stageID string) *config.DeliverySpec {
	for _, edge := range p.ir.Edges {
		if edge.To == stageID && edge.Delivery != nil {
			return edge.Delivery
		}
	}
	return nil
}

// writeWithRetry returns the number of retries actually performed (0 on
// first-attempt success) alongside the error.
func (p *Pipeline) writeWithRetry(ctx context.Context, sk stage.Sink, batch []*message.Message, delivery *config.DeliverySpec) (int, error) {
	maxRetries := 0
	backoff := "exponential"
	if delivery != nil && delivery.Retry != nil {
		maxRetries = delivery.Retry.Max
		if delivery.Retry.Backoff != "" {
			backoff = delivery.Retry.Backoff
		}
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = sk.Write(ctx, batch)
		if lastErr == nil {
			return attempt, nil
		}
		if attempt < maxRetries {
			delay := retryDelay(attempt, backoff)
			select {
			case <-ctx.Done():
				return attempt, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return maxRetries, fmt.Errorf("retries exhausted (%d attempts): %w", maxRetries+1, lastErr)
}

func retryDelay(attempt int, backoff string) time.Duration {
	base := 100 * time.Millisecond
	switch backoff {
	case "exponential":
		return time.Duration(math.Pow(2, float64(attempt))) * base
	case "linear":
		return time.Duration(attempt+1) * base
	default:
		return base
	}
}

// deliverToDLQ routes a failed batch along the DLQ fallback chain (design
// §6.5): inbound edge delivery.dlq → pipeline-level dlq.sink → drop.
//
// It returns one entry per message: true when the message reached a DLQ sink
// (the failure is disposed of, so the original message must be acked with
// nil — Kafka Connect semantics), false when it was dropped (no DLQ
// configured) or the DLQ write itself failed (keep the nack so the source
// can redeliver). Nil is returned when no DLQ stage was usable at all.
func (p *Pipeline) deliverToDLQ(batch []*message.Message, err error, sourceStageID string, delivery *config.DeliverySpec, retries int) []bool {
	dlqSink, dlqSinkID := p.resolveDLQSink(delivery)
	if dlqSink == nil {
		// No DLQ configured (or none of the referenced stages is a usable
		// sink) — messages are dropped (already acked with error)
		return nil
	}
	if p.metrics != nil {
		p.metrics.IncDLQ(p.ir.Name, dlqSinkID, "sink_write_error")
	}
	// DLQ writes run under a bounded context and failures are logged, never
	// silently swallowed.
	d := p.newDrainWriteCtx()
	defer d.cancel()
	now := time.Now().UTC().Format(time.RFC3339)
	delivered := make([]bool, len(batch))
	for i, m := range batch {
		meta := map[string]any{
			"er-error-reason":      err.Error(),
			"er-error-type":        "sink_write_error",
			"er-error-stage":       sourceStageID,
			"er-error-timestamp":   now,
			"er-original-pipeline": p.ir.Name,
			"er-retry-count":       strconv.Itoa(retries),
		}
		if src := m.SourceStageID(); src != "" {
			meta["er-original-source"] = src
		}
		// kafka provenance, when the source recorded it (§6.5 — 如适用)
		if v, ok := m.Metadata["kafka.topic"]; ok {
			meta["er-original-topic"] = v
		}
		if v, ok := m.Metadata["kafka.partition"]; ok {
			meta["er-original-partition"] = v
		}
		if v, ok := m.Metadata["kafka.offset"]; ok {
			meta["er-original-offset"] = v
		}
		if p.ir.DLQ != nil && p.ir.DLQ.IncludeCurrentPayload {
			meta["er-current-payload"] = base64.StdEncoding.EncodeToString(m.Payload)
		}
		dlqMsg := message.New(m.OriginalPayload(), meta)
		dlqMsg.ID = uuid.NewString()
		if werr := dlqSink.Write(d.ctx, []*message.Message{dlqMsg}); werr != nil {
			slog.Warn("dlq write failed, message dropped",
				"pipeline", p.ir.Name, "dlq_sink", dlqSinkID, "msg", m.ID, "err", werr)
			continue
		}
		delivered[i] = true
	}
	return delivered
}

// resolveDLQSink walks the fallback chain and returns the first configured
// DLQ reference that points at a usable sink stage.
func (p *Pipeline) resolveDLQSink(delivery *config.DeliverySpec) (stage.Sink, string) {
	candidates := [2]string{}
	if delivery != nil {
		candidates[0] = delivery.DLQ
	}
	if p.ir.DLQ != nil {
		candidates[1] = p.ir.DLQ.Sink
	}
	for _, id := range candidates {
		if id == "" {
			continue
		}
		st, ok := p.stages[id]
		if !ok {
			slog.Warn("dlq sink stage not found, trying next fallback",
				"pipeline", p.ir.Name, "dlq_sink", id)
			continue
		}
		if sink, ok := st.(stage.Sink); ok {
			return sink, id
		}
	}
	return nil, ""
}

func (p *Pipeline) Name() string {
	return p.ir.Name
}

func (p *Pipeline) Inflight() int32 {
	return p.inflight.Load()
}

func (p *Pipeline) CheckStages(ctx context.Context) []observability.StageHealth {
	out := make([]observability.StageHealth, 0, len(p.stages))
	for id, st := range p.stages {
		out = append(out, observability.CheckStage(p.ir.Name, id, st, ctx))
	}
	return out
}
