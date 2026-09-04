// Package engine executes one compiled pipeline: source admission through a
// durable spool, in-memory DAG execution with settle tracking, per-edge
// delivery retries, dead lettering, checkpointing and backpressure
// (redesign-v3.md §6.2). Every reliability property is phrased as one of the
// seven invariants, each with a dedicated test.
package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/rpcplugin"
	"github.com/eventboat/eventboat/internal/store"
)

// Options tunes engine behavior. Tests use tiny backoffs and fixed clocks.
type Options struct {
	Clock          func() time.Time
	NewID          func() string
	BackoffBase    time.Duration // delivery retry backoff base (exponential)
	DLBackoff      time.Duration // dead letter write retry interval
	HighWatermark  int           // unsettled messages before sources pause
	ChannelSize    int           // default per-node channel capacity
	BatchFlush     time.Duration // sink batch flush interval
	DefaultTimeout time.Duration // per sink-write attempt when unset on edge
	DrainTimeout   time.Duration // graceful drain bound before hard cancel
	StarOptions    starhost.Options

	// Obs receives OpenTelemetry events (nil-safe: nil disables telemetry).
	Obs *obs.Obs

	// OnSourceError reports a pull-source failure (job pipelines route this
	// to a failed run; continuous sources have no error channel).
	OnSourceError func(node string, err error)

	// MetaStamps are stamped into every accepted message's metadata (e.g.
	// job_run_id for job runs).
	MetaStamps map[string]any

	// DisableSources keeps registered sources from running (testkit/
	// contract tests drive injections instead). Injection at source nodes
	// still goes through the full accept path.
	DisableSources bool

	// SinkWrapper lets testkits capture or fault-inject around real sinks.
	SinkWrapper func(node string, s registry.Sink) registry.Sink

	// Logf surfaces plugin-process output and out-of-process source stream
	// errors (nil = discard).
	Logf func(format string, args ...any)
}

// DefaultOptions returns production defaults.
func DefaultOptions() Options {
	return Options{
		Clock:          time.Now,
		NewID:          func() string { return uuid.New().String() },
		BackoffBase:    100 * time.Millisecond,
		DLBackoff:      500 * time.Millisecond,
		HighWatermark:  10_000,
		ChannelSize:    128,
		BatchFlush:     time.Second,
		DefaultTimeout: 30 * time.Second,
		DrainTimeout:   10 * time.Second,
		StarOptions:    starhost.DefaultOptions(),
	}
}

// WithLimits applies the pipeline-level limits section on top of base options
// (redesign-v3.md §5.10: max_in_flight caps spool admission, drain_timeout
// bounds graceful shutdown).
func (o Options) WithLimits(l *config.Limits) Options {
	if l == nil {
		return o
	}
	if l.MaxInFlight > 0 {
		o.HighWatermark = l.MaxInFlight
	}
	if l.DrainTimeout > 0 {
		o.DrainTimeout = l.DrainTimeout
	}
	return o
}

// Metrics holds engine counters (POC observability: expvar-style atomics).
// SettledCount counts messages settled this engine instance; CheckpointPtr
// mirrors the durable checkpoint position (M2 review R5 split them apart).
type Metrics struct {
	MessagesIn    atomic.Int64
	SettledCount  atomic.Int64
	CheckpointPtr atomic.Int64
	DeadLettered  atomic.Int64
	CelEvalErrors atomic.Int64
	NoMatch       atomic.Int64
	Retries       atomic.Int64
	DlqFailures   atomic.Int64
	OptionalDrops atomic.Int64
	DecodeErrors  atomic.Int64
	TransformRuns atomic.Int64
	Backpressured atomic.Int64
	SpoolFailures atomic.Int64
}

// Engine runs one pipeline against one store.
type Engine struct {
	IR      *ir.Pipeline
	Store   store.Store
	Reg     *registry.Registry
	Opts    Options
	Metrics Metrics

	settle  *settleTracker
	chans   map[string]chan *instance
	sinks   map[string]registry.Sink
	codecs  map[string]registry.Codec // resolved by codec name
	sources map[string]registry.Source

	admitSem chan struct{}

	acquired   map[int64]bool // spool seqs holding an admission slot
	acquiredMu sync.Mutex

	acceptMu   sync.Mutex
	acceptedAt map[int64]time.Time // spool seq → accept time (settle latency)

	persistMu        sync.Mutex
	persistedThrough int64

	srcWG    sync.WaitGroup // live source goroutines (exhaustion tracking)
	srcErrMu sync.Mutex
	srcErr   map[string]error // first error per pull source
	srcDone  map[string]bool  // sources that returned (exhausted or failed)
	srcTotal int
	srcStart atomic.Bool // Run counted the sources; SourcesDone is meaningful

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
}

// instance is one in-flight message positioned at a node.
type instance struct {
	seq  int64 // spool sequence
	msg  registry.Message
	via  *ir.Edge // edge it arrived on (nil at entry)
	node string
}

// New builds an engine: resolves plugins and codecs, allocates channels and
// the settle tracker. Call Run to start it.
func New(p *ir.Pipeline, st store.Store, reg *registry.Registry, opts Options) (*Engine, error) {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.NewID == nil {
		opts.NewID = func() string { return uuid.New().String() }
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 100 * time.Millisecond
	}
	if opts.DLBackoff <= 0 {
		opts.DLBackoff = 500 * time.Millisecond
	}
	if opts.HighWatermark <= 0 {
		opts.HighWatermark = 10_000
	}
	if opts.ChannelSize <= 0 {
		opts.ChannelSize = 128
	}
	if opts.BatchFlush <= 0 {
		opts.BatchFlush = time.Second
	}
	if opts.DefaultTimeout <= 0 {
		opts.DefaultTimeout = 30 * time.Second
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 10 * time.Second
	}

	e := &Engine{
		IR:         p,
		Store:      st,
		Reg:        reg,
		Opts:       opts,
		chans:      map[string]chan *instance{},
		sinks:      map[string]registry.Sink{},
		codecs:     map[string]registry.Codec{},
		sources:    map[string]registry.Source{},
		acquired:   map[int64]bool{},
		srcErr:     map[string]error{},
		srcDone:    map[string]bool{},
		acceptedAt: map[int64]time.Time{},
	}

	for _, name := range p.Order {
		n := p.Nodes[name]
		switch n.Section {
		case config.SectionSource:
			var src registry.Source
			var err error
			if n.Config.Grpc != nil {
				src, err = rpcplugin.SpawnSource(context.Background(), n.Config.Grpc, n.Config.Manifest, n.Config.PluginConfig, opts.Logf)
			} else {
				src, err = reg.NewSource(n.Config.Plugin, n.Config.PluginConfig)
			}
			if err != nil {
				return nil, fmt.Errorf("source %q: %w", name, err)
			}
			e.sources[name] = src
			codecName := n.Config.Decoder
			if codecName == "" {
				codecName = "json"
			}
			if _, err := e.codec(codecName, reg); err != nil {
				return nil, fmt.Errorf("source %q: %w", name, err)
			}
		case config.SectionSink:
			var sink registry.Sink
			var err error
			if n.Config.Grpc != nil {
				sink, err = rpcplugin.SpawnSink(context.Background(), n.Config.Grpc, n.Config.Manifest, n.Config.PluginConfig, opts.Logf)
			} else {
				sink, err = reg.NewSink(n.Config.Plugin, n.Config.PluginConfig)
			}
			if err != nil {
				return nil, fmt.Errorf("sink %q: %w", name, err)
			}
			if opts.SinkWrapper != nil {
				sink = opts.SinkWrapper(name, sink)
			}
			e.sinks[name] = sink
			codecName := n.Config.Encoder
			if codecName == "" {
				codecName = "json"
			}
			if _, err := e.codec(codecName, reg); err != nil {
				return nil, fmt.Errorf("sink %q: %w", name, err)
			}
			capacity := opts.ChannelSize
			for _, edge := range n.In {
				if edge.BufferMax > 0 && edge.BufferMax < capacity*4 {
					// per-edge buffer sizing acts as surge capacity (§6.2)
					capacity = maxInt(capacity, edge.BufferMax)
				}
			}
			e.chans[name] = make(chan *instance, capacity)
		case config.SectionTransform:
			workers := n.Config.Workers
			if workers < 1 {
				workers = 1
			}
			capacity := opts.ChannelSize
			for _, edge := range n.In {
				if edge.BufferMax > 0 {
					capacity = maxInt(capacity, edge.BufferMax)
				}
			}
			e.chans[name] = make(chan *instance, capacity)
			_ = workers // used at Run
		}
	}

	var sourceNames []string
	for _, name := range p.Order {
		if p.Nodes[name].Section == config.SectionSource {
			sourceNames = append(sourceNames, name)
		}
	}
	e.settle = newSettleTracker(p.Config.Name, sourceNames, e.onSettled, e.persistCheckpoint)
	e.admitSem = make(chan struct{}, opts.HighWatermark)
	return e, nil
}

// onSettled releases the backpressure slot of one settled message, counts
// it (R5: a count, distinct from the checkpoint pointer) and records its
// accept-to-settle latency.
func (e *Engine) onSettled(seq int64) {
	e.Metrics.SettledCount.Add(1)
	e.acceptMu.Lock()
	accepted := e.acceptedAt[seq]
	delete(e.acceptedAt, seq)
	e.acceptMu.Unlock()
	latency := time.Duration(0)
	if !accepted.IsZero() {
		latency = e.Opts.Clock().Sub(accepted)
	}
	e.Opts.Obs.RecordSettled(e.IR.Config.Name, latency)
	e.releaseAdmission(seq)
}

// persistCheckpoint advances the durable checkpoint (invariant 2) and pushes
// settle notifications into sources, persisting their returned state.
// frontiers are precomputed by the tracker under its lock; a mutex plus a
// monotonic guard keep interleaved advances from regressing the checkpoint.
func (e *Engine) persistCheckpoint(settledThrough int64, frontiers map[string]int64) {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	if settledThrough > e.persistedThrough {
		if err := e.Store.SetCheckpoint(e.IR.Config.Name, settledThrough); err != nil {
			// A failed checkpoint persist only widens the replay window on
			// crash; the next advance retries. Never a loss (invariant 3).
			return
		}
		e.persistedThrough = settledThrough
	}
	e.Metrics.CheckpointPtr.Store(settledThrough)
	for name, src := range e.sources {
		frontier := frontiers[name]
		if frontier <= 0 {
			continue
		}
		state, err := src.Settled(e.ctx, frontier)
		if err != nil || state == nil {
			continue
		}
		_ = e.Store.SetSourceState(e.IR.Config.Name, name, state, frontier)
	}
}

// releaseAdmission frees the backpressure slot acquired when the message was
// spooled. Sequences that never acquired a slot (replayed rows) are skipped.
func (e *Engine) releaseAdmission(seq int64) {
	e.acquiredMu.Lock()
	if !e.acquired[seq] {
		e.acquiredMu.Unlock()
		return
	}
	delete(e.acquired, seq)
	e.acquiredMu.Unlock()
	<-e.admitSem
}

func (e *Engine) codec(name string, reg *registry.Registry) (registry.Codec, error) {
	if c, ok := e.codecs[name]; ok {
		return c, nil
	}
	c, err := reg.NewCodec(name, nil)
	if err != nil {
		return nil, err
	}
	e.codecs[name] = c
	return c, nil
}

// srcFrontier reports the contiguous settled srcSeq of one source (tests).
func (e *Engine) srcFrontierOf(name string) int64 { return e.settle.srcFrontier(name) }

// Run replays the spool beyond the checkpoint, starts sources and workers,
// and blocks until ctx is done.
func (e *Engine) Run(ctx context.Context) error {
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.started.Store(true)

	// Crash recovery: replay everything beyond the checkpoint (invariant 3).
	cp, err := e.Store.Checkpoint(e.IR.Config.Name)
	if err != nil {
		return fmt.Errorf("engine: read checkpoint: %w", err)
	}
	if err := e.Store.ReplayFrom(e.IR.Config.Name, cp, func(seq int64, msg registry.Message, ingestTime time.Time) error {
		node, _ := msg.Meta["source"].(string)
		if _, known := e.IR.Nodes[node]; !known {
			// Spooled but never dispatched and not attributable: release it
			// instead of wedging the contiguous prefix.
			e.settle.arrived(seq, "", 0)
			e.settle.done(seq)
			return nil
		}
		e.settle.arrived(seq, "", 0)
		e.dispatchFrom(node, seq, msg)
		return nil
	}); err != nil {
		return fmt.Errorf("engine: replay: %w", err)
	}

	// Workers: transforms then sinks.
	for _, name := range e.IR.Order {
		n := e.IR.Nodes[name]
		switch n.Section {
		case config.SectionTransform:
			workers := n.Config.Workers
			if workers < 1 {
				workers = 1
			}
			for i := 0; i < workers; i++ {
				e.wg.Add(1)
				go e.runTransform(n)
			}
		case config.SectionSink:
			e.wg.Add(1)
			go e.runSink(n)
		}
	}

	// Sources last: fresh state, then run. Pull sources (job pipelines) use
	// Pull and signal exhaustion or failure; the job runner watches
	// SourcesDone/SourceErrors for run completion (M2 review R1).
	if e.Opts.DisableSources {
		e.srcErrMu.Lock()
		e.srcTotal = len(e.sources)
		for name := range e.sources {
			e.srcDone[name] = true
		}
		e.srcStart.Store(true)
		e.srcErrMu.Unlock()
	} else {
		e.srcErrMu.Lock()
		e.srcTotal = len(e.sources)
		e.srcStart.Store(true) // set before goroutines spawn: SourcesDone is only meaningful once counted
		e.srcErrMu.Unlock()
		for name, src := range e.sources {
			state, _, err := e.Store.SourceState(e.IR.Config.Name, name)
			if err == nil && len(state) > 0 {
				_ = src.Init(state)
			}
			e.srcWG.Add(1)
			go func(name string, src registry.Source) {
				defer e.srcWG.Done()
				if ps, ok := src.(registry.PullSource); ok {
					err := ps.Pull(e.ctx, func(msg registry.Message) {
						_ = e.accept(msg, name)
					})
					e.markSourceDone(name, err)
					if err != nil && e.Opts.OnSourceError != nil {
						e.Opts.OnSourceError(name, err)
					}
				} else {
					src.Run(e.ctx, func(msg registry.Message) {
						_ = e.accept(msg, name)
					})
					e.markSourceDone(name, nil)
				}
				_ = src.Close()
			}(name, src)
		}
	}

	<-e.ctx.Done()
	e.drain()
	return nil
}

// drain waits up to DrainTimeout for in-flight work, then hard-cancels.
func (e *Engine) drain() {
	done := make(chan struct{})
	go func() { e.wg.Wait(); e.srcWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(e.Opts.DrainTimeout):
		e.cancel()
		<-done
	}
	for _, name := range e.IR.Order {
		if e.IR.Nodes[name].Section == config.SectionSink {
			if s, ok := e.sinks[name]; ok {
				_ = s.Close()
			}
		}
	}
}

// Close cancels the engine (idempotent).
func (e *Engine) Close() {
	if e.cancel != nil {
		e.cancel()
	}
}

// Ready reports whether Run has begun accepting injections.
func (e *Engine) Ready() bool { return e.started.Load() }

// markSourceDone records that one source goroutine returned (exhausted pull,
// failed pull, or stopped continuous source).
func (e *Engine) markSourceDone(name string, err error) {
	e.srcErrMu.Lock()
	defer e.srcErrMu.Unlock()
	e.srcDone[name] = true
	if err != nil {
		if _, had := e.srcErr[name]; !had {
			e.srcErr[name] = err
		}
	}
}

// SourcesDone reports whether every registered source has stopped (job
// completion detection: sources exhausted AND settle outstanding == 0).
// False until Run has counted the sources — a poll racing engine startup
// must not read "all done" from a zero total.
func (e *Engine) SourcesDone() bool {
	e.srcErrMu.Lock()
	defer e.srcErrMu.Unlock()
	return e.srcStart.Load() && len(e.srcDone) >= e.srcTotal
}

// SourceErrors returns the first failure per pull source (empty when none).
func (e *Engine) SourceErrors() map[string]error {
	e.srcErrMu.Lock()
	defer e.srcErrMu.Unlock()
	out := make(map[string]error, len(e.srcErr))
	for k, v := range e.srcErr {
		out[k] = v
	}
	return out
}

// Quiesced reports whether the pipeline has no outstanding execution work:
// all sources stopped and nothing unsettled. Job runners poll this to move a
// run into its terminal state.
func (e *Engine) Quiesced() bool {
	if !e.SourcesDone() {
		return false
	}
	outstanding, _, _ := e.settle.snapshot()
	return outstanding == 0
}

// Abandon terminal-settles every outstanding message by dead-lettering it
// (M2 review R2: a canceled run must not leave unsettled spool rows wedging
// the checkpoint's contiguous prefix forever). Ordering preserves the
// invariants: the dead letter is written first, and only then does the
// tracker clear the message (any leftover branches from a mid-flight fan-out
// are force-terminated after the durable record exists).
func (e *Engine) Abandon(reason string) int {
	abandoned := 0
	_, settledThrough, _ := e.settle.snapshot()
	after := settledThrough
	for {
		var seqs []int64
		msgs := map[int64]registry.Message{}
		last, more, ferr := e.Store.ReplayPage(e.IR.Config.Name, after, 256,
			func(seq int64, msg registry.Message, _ time.Time) error {
				if e.settle.isOutstanding(seq) {
					seqs = append(seqs, seq)
					msgs[seq] = msg
				}
				return nil
			})
		if ferr != nil {
			break
		}
		for _, seq := range seqs {
			msg := msgs[seq]
			e.deadLetterMsg(seq, msg, firstNonEmpty(msg.SrcName, "unknown"), "", reason, "")
			e.settle.forceTerminal(seq)
			abandoned++
		}
		if !more || last == after {
			break
		}
		after = last
	}
	return abandoned
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// accept is the single entry point for source emissions: backpressure gate,
// engine stamping, durable spool append, then DAG visibility (invariant 1:
// nothing is visible before the append succeeds).
func (e *Engine) accept(raw registry.Message, sourceNode string) error {
	// Backpressure: block while too many unsettled messages are in flight.
	select {
	case e.admitSem <- struct{}{}:
	case <-e.ctx.Done():
		e.Metrics.Backpressured.Add(1)
		e.Opts.Obs.RecordBackpressure(e.IR.Config.Name, sourceNode)
		return e.ctx.Err()
	}

	msg := raw
	msg.SrcName = sourceNode
	if msg.ID == "" {
		msg.ID = e.Opts.NewID()
	}
	ingest := e.Opts.Clock()
	meta := cloneMeta(msg.Meta)
	meta["message_id"] = msg.ID
	meta["ingest_time"] = ingest.UTC().Format(time.RFC3339Nano)
	meta["source"] = sourceNode
	for k, v := range e.Opts.MetaStamps {
		if _, exists := meta[k]; !exists {
			meta[k] = v
		}
	}
	msg.Meta = meta
	node := e.IR.Nodes[sourceNode]
	codecName := node.Config.Decoder
	if codecName == "" {
		codecName = "json"
	}
	msg.Codec = codecName

	e.Metrics.MessagesIn.Add(1)
	e.Opts.Obs.RecordMessageIn(e.IR.Config.Name, sourceNode)
	seq, err := e.Store.AppendSpool(e.IR.Config.Name, msg, ingest)
	if err != nil {
		<-e.admitSem
		e.Metrics.SpoolFailures.Add(1)
		e.Opts.Obs.RecordSpoolFailure(e.IR.Config.Name)
		return fmt.Errorf("engine: spool append failed; message NOT delivered: %w", err)
	}
	e.acceptMu.Lock()
	e.acceptedAt[seq] = ingest
	e.acceptMu.Unlock()
	e.acquiredMu.Lock()
	e.acquired[seq] = true
	e.acquiredMu.Unlock()
	e.settle.arrived(seq, sourceNode, raw.SrcSeq)
	e.dispatchFrom(sourceNode, seq, msg)
	return nil
}

// dispatchFrom decodes (at source entry) and fans a spooled message out of
// its originating node.
func (e *Engine) dispatchFrom(sourceNode string, seq int64, msg registry.Message) {
	node := e.IR.Nodes[sourceNode]
	codec, err := e.codec(msg.Codec, e.Reg)
	if err != nil {
		e.deadLetterMsg(seq, msg, sourceNode, "", "codec: "+err.Error(), "")
		return
	}
	if msg.Decoded == nil {
		v, derr := codec.Decode(msg.Raw)
		if derr != nil {
			e.Metrics.DecodeErrors.Add(1)
			e.Opts.Obs.RecordDecodeError(e.IR.Config.Name, sourceNode)
			e.deadLetterMsg(seq, msg, sourceNode, "", "decode: "+derr.Error(), "")
			return
		}
		msg.Decoded = v
	}
	e.fanOut(node, seq, msg)
}

// fanOut evaluates outgoing edge conditions (CEL errors count and act as
// not-passed) and delivers to every matched edge. Zero matches settle the
// message as filtered (documented semantics, review R7).
func (e *Engine) fanOut(node *ir.Node, seq int64, msg registry.Message) {
	matched := make([]*ir.Edge, 0, len(node.Out))
	for i := range node.Out {
		edge := &node.Out[i]
			if edge.When != nil {
				ok, evalErr := edge.When.Eval(msg.Decoded, msg.Meta)
				if evalErr != nil {
					e.Metrics.CelEvalErrors.Add(1)
					e.Opts.Obs.RecordCelError(e.IR.Config.Name, edge.From+" -> "+edge.To, edge.When.Lang())
					continue
				}
				if !ok {
					continue
				}
			}
		matched = append(matched, edge)
	}
	if len(matched) == 0 {
		e.Metrics.NoMatch.Add(1)
		e.Opts.Obs.RecordNoMatch(e.IR.Config.Name, node.Name)
		e.settle.done(seq)
		return
	}
	e.settle.add(seq, len(matched)-1)
	for _, edge := range matched {
		e.deliver(edge, seq, msg)
	}
}

func (e *Engine) deliver(edge *ir.Edge, seq int64, msg registry.Message) {
	inst := &instance{seq: seq, msg: msg, via: edge, node: edge.To}
	select {
	case e.chans[edge.To] <- inst:
	case <-e.ctx.Done():
		// Shutdown with undelivered work: deliberately NOT settled, it stays
		// unsettled and will be replayed from the spool on restart
		// (invariant 3: replay covers the unsettled set).
	}
}

// InjectAt feeds a message into a node: at a source it goes through the full
// accept path (spool + stamps); at an internal node it is spooled and enters
// the DAG at that node (testkit / replay).
func (e *Engine) InjectAt(node string, raw []byte, meta map[string]any) (int64, error) {
	return e.injectAt(node, raw, meta, "")
}

// InjectReplay re-injects one previously dead-lettered (or spooled) message
// (§3.3): it enters at the given node, keeps its ORIGINAL message_id (so
// idempotent sinks deduplicate re-deliveries) and is stamped
// meta.is_replay=true for sinks to recognize.
func (e *Engine) InjectReplay(node string, raw []byte, meta map[string]any, originalID string) (int64, error) {
	if meta == nil {
		meta = map[string]any{}
	}
	meta = cloneMeta(meta)
	meta["is_replay"] = true
	if originalID != "" {
		meta["original_message_id"] = originalID
	}
	return e.injectAt(node, raw, meta, originalID)
}

func (e *Engine) injectAt(node string, raw []byte, meta map[string]any, keepID string) (int64, error) {
	n, ok := e.IR.Nodes[node]
	if !ok {
		return 0, fmt.Errorf("engine: unknown node %q", node)
	}
	if !e.started.Load() {
		return 0, fmt.Errorf("engine: not started; call Run first")
	}
	if n.Section == config.SectionSource {
		// accept() preserves a non-empty message_id, so replays keep their
		// original identity through the full spool path.
		if err := e.accept(registry.Message{Raw: raw, Meta: meta, SrcSeq: 0, ID: keepID}, node); err != nil {
			return 0, err
		}
		return 0, nil
	}
	// Internal injection: spool with the entry node as dispatch origin; the
	// message enters the DAG at that node, skipping its upstream.
	msg := registry.Message{Raw: raw, Meta: meta, SrcSeq: 0}
	msg.ID = e.Opts.NewID()
	if keepID != "" {
		msg.ID = keepID
	}
	ingest := e.Opts.Clock()
	m := cloneMeta(msg.Meta)
	m["message_id"] = msg.ID
	m["ingest_time"] = ingest.UTC().Format(time.RFC3339Nano)
	m["source"] = node
	m["injected_at"] = node
	for k, v := range e.Opts.MetaStamps {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	msg.Meta = m
	msg.Codec = "json"
	select {
	case e.admitSem <- struct{}{}:
	case <-e.ctx.Done():
		return 0, e.ctx.Err()
	}
	seq, err := e.Store.AppendSpool(e.IR.Config.Name, msg, ingest)
	if err != nil {
		<-e.admitSem
		return 0, err
	}
	e.acquiredMu.Lock()
	e.acquired[seq] = true
	e.acquiredMu.Unlock()
	e.settle.arrived(seq, "", 0)
	// Decode once so the sink's encoder can re-encode (writeBatch encodes
	// from Decoded; Raw is the spooled truth and stays untouched).
	codec, cerr := e.codec("json", e.Reg)
	if cerr != nil {
		e.deadLetterMsg(seq, msg, node, "", "codec: "+cerr.Error(), "")
		return seq, nil
	}
	v, derr := codec.Decode(msg.Raw)
	if derr != nil {
		e.deadLetterMsg(seq, msg, node, "", "decode: "+derr.Error(), "")
		return seq, nil
	}
	msg.Decoded = v
	// Injection enters INTO the node: transforms run their script (a replay
	// after a script fix re-executes it), sinks batch and write under their
	// delivery policy (M2 review R4: replay can target any node). The single
	// outstanding branch registered by arrived is exactly this one delivery.
	e.deliver(&ir.Edge{From: node, To: node}, seq, msg)
	return seq, nil
}

// WaitSettled blocks until no outstanding branches remain (test helper).
func (e *Engine) WaitSettled(ctx context.Context) error {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		outstanding, _, _ := e.settle.snapshot()
		if outstanding == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			outstanding, settled, arrived := e.settle.snapshot()
			return fmt.Errorf("wait settled: ctx done with %d outstanding (settledThrough=%d arrivedMax=%d)", outstanding, settled, arrived)
		case <-tick.C:
		}
	}
}

// SettleSnapshot exposes settle counters for tests and status.
func (e *Engine) SettleSnapshot() (outstanding int, settledThrough int64, arrivedMax int64) {
	return e.settle.snapshot()
}

func cloneMeta(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+4)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
