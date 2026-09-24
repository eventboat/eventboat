// Package engine executes one compiled pipeline: source admission through a
// durable spool, in-memory DAG execution with commit tracking, per-edge
// delivery retries, dead lettering, checkpointing and backpressure
// (redesign-v3.md §6.2). Every reliability property is phrased as one of the
// seven invariants, each with a dedicated test.
package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/rpcplugin"
	"github.com/eventboat/eventboat/internal/store"
)

// pluginCfgMap narrows a node's plugin block to a mapping; parse guarantees
// mappings for source/sink blocks (transform configs may be scalars).
func pluginCfgMap(n *config.Node) map[string]any {
	m, _ := n.PluginConfig.(map[string]any)
	return m
}

// Options tunes engine behavior. Tests use tiny backoffs and fixed clocks.
type Options struct {
	Clock          func() time.Time
	NewID          func() string
	BackoffBase    time.Duration // delivery retry backoff base (exponential)
	DLBackoff      time.Duration // dead letter write retry interval
	HighWatermark  int           // uncommitted messages before sources pause
	ChannelSize    int           // default per-node channel capacity
	BatchFlush     time.Duration // sink batch flush interval
	DefaultTimeout time.Duration // per sink-write attempt when unset on edge
	DrainTimeout   time.Duration // graceful drain bound before hard cancel
	StarOptions    starhost.Options

	// SpoolRetention bounds the spool: once the DURABLE checkpoint reaches C,
	// rows at or below C - SpoolRetention are history and get deleted (one
	// batched sweep per window of advance, on the checkpoint flush path).
	// 0 = DefaultSpoolRetention. Crash recovery replays only beyond the
	// checkpoint, so the replay tail is never trimmed; manual replay keeps
	// the retained window.
	SpoolRetention int64

	// DLQRetention prunes dead letters older than this duration (the
	// pipeline's dlq.retention, §5.10), swept on the same checkpoint window
	// as the spool trim. OPT-IN with no default: dead letters are operator
	// data for `replay`, so 0 keeps everything forever. New() falls back to
	// the IR's dlq.retention when this is 0; a positive value here overrides
	// the pipeline config (tests, or a caller with its own policy).
	DLQRetention time.Duration

	// Admission optionally replaces the per-engine admission semaphore with a
	// SHARED pool (same capacity semantics: one slot per uncommitted message).
	// The jobs manager hands one pool to every concurrent run of a pipeline
	// so max_in_flight aggregates across overlap:all runs instead of
	// multiplying per run (M2 review R17). nil = per-engine semaphore.
	Admission chan struct{}

	// SpanSampleRate is the per-message span sampling rate (pipeline
	// telemetry.span_sample_rate, §6.6/R16: per-message spans only when the
	// pipeline opts in). 0 (default) = no spans, zero cost; 1 = all
	// messages. Spans cover accept → terminal state (committed or dead
	// letter); they are roots (the engine does not thread the span context
	// through the DAG — correlation rides the attributes).
	SpanSampleRate float64

	// Obs receives OpenTelemetry events (nil-safe: nil disables telemetry).
	Obs *obs.Obs

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

	// WasmSlowCallWarnMs arms the zero-interference wasm slow-call watchdog
	// (log once per long-running invoke). 0 = default (5000, normalized by
	// New like every other numeric option); a negative value disables.
	WasmSlowCallWarnMs int
}

// DefaultHighWatermark is the spool admission limit applied when neither the
// pipeline's limits section nor Options set one. Exported so pool owners
// (the jobs manager's aggregated admission) size identically to engine.New.
const DefaultHighWatermark = 10_000

// DefaultSpoolRetention is how many spool rows stay behind the checkpoint
// when neither the Runtime config nor Options set spool retention: enough
// recent history for `replay --spool` disaster drills, while bounding disk
// (SQLite) and memory (--ephemeral) on long runs.
const DefaultSpoolRetention = 10_000

// DefaultOptions returns production defaults. It is `Options{}.withDefaults()`
// — the ONE runtime normalization path New also applies, so a hand-built
// Options{} cannot drift from the documented defaults (candidate 06).
func DefaultOptions() Options { return Options{}.withDefaults() }

// withDefaults fills every unset runtime knob with its production default:
// zero (or negative, where zero is meaningless) means "unset", except
// WasmSlowCallWarnMs where a negative value explicitly disables the watchdog.
// The zero-value StarOptions carries MaxSteps 0, which starhost.Compile also
// treats as "the documented default budget"; setting it here keeps the two
// layers agreeing.
func (o Options) withDefaults() Options {
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.NewID == nil {
		o.NewID = func() string { return uuid.NewString() }
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 100 * time.Millisecond
	}
	if o.DLBackoff <= 0 {
		o.DLBackoff = 500 * time.Millisecond
	}
	if o.HighWatermark <= 0 {
		o.HighWatermark = DefaultHighWatermark
	}
	if o.ChannelSize <= 0 {
		o.ChannelSize = 128
	}
	if o.BatchFlush <= 0 {
		o.BatchFlush = time.Second
	}
	if o.DefaultTimeout <= 0 {
		o.DefaultTimeout = 30 * time.Second
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 10 * time.Second
	}
	if o.SpoolRetention <= 0 {
		o.SpoolRetention = DefaultSpoolRetention
	}
	if o.WasmSlowCallWarnMs == 0 {
		// Negative explicitly disables; zero keeps the default watchdog so a
		// hand-built Options{} does not silently lose it (review-2026-09).
		o.WasmSlowCallWarnMs = 5000
	}
	if o.StarOptions.MaxSteps == 0 {
		o.StarOptions = starhost.DefaultOptions()
	}
	return o
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

// RunStatus is the terminal classification of one run (CONTEXT.md "Run
// outcome"): completed (quiesced cleanly), partial (quiesced with dead
// letters), failed (the engine stopped itself: worker-fatal or a source
// failure) or interrupted (the caller canceled the run).
type RunStatus string

const (
	RunCompleted   RunStatus = "completed"
	RunPartial     RunStatus = "partial"
	RunFailed      RunStatus = "failed"
	RunInterrupted RunStatus = "interrupted"
)

// Outcome is the engine's report of how one run ended — the single decision
// point every runner reads (candidate 02). Counts are the engine's own
// metrics; SourceErrors is the race-free snapshot map; WorkerFatal is the
// error Run returned when the engine stopped itself; Abandoned/AbandonError
// report the cancel-time abandon attempt (jobs' R2 semantics).
type Outcome struct {
	Status       RunStatus
	RowsRead     int64
	Committed    int64
	DeadLettered int64
	SourceErrors map[string]error
	WorkerFatal  error
	Abandoned    int
	AbandonError error
}

// FailureText renders a failed outcome's cause: the worker-fatal error first,
// then the first source error in stable node order. It is the operator-facing
// summary jobs/ops/CLI report; empty for a non-failed outcome.
func (o Outcome) FailureText() string {
	if o.WorkerFatal != nil {
		return o.WorkerFatal.Error()
	}
	if len(o.SourceErrors) == 0 {
		return ""
	}
	nodes := make([]string, 0, len(o.SourceErrors))
	for node := range o.SourceErrors {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return fmt.Sprintf("source %q failed: %v", nodes[0], o.SourceErrors[nodes[0]])
}

// WaitOptions tunes Engine.Wait.
type WaitOptions struct {
	// AbandonOnCancel terminal-dead-letters the outstanding set when the
	// CALLER's ctx canceled the run (job runs: the R2 semantics). Batch runs
	// leave it false — uncommitted rows replay on the next run, and dead
	// letters are operator data, not garbage.
	AbandonOnCancel bool
	// AbandonReason is the dead-letter reason the abandon path records
	// (default "run canceled").
	AbandonReason string
	// AbandonTimeout bounds the abandon attempt (0 = the engine's
	// DrainTimeout). The caller's ctx is already canceled on that path, so
	// Wait derives a fresh bounded context; the write-first contract means a
	// timeout leaves every unrecorded message uncommitted (never loss).
	AbandonTimeout time.Duration
	// DrainTimeout bounds the post-cancel wait for Run to return (0 = the
	// engine's DrainTimeout + 5s, the racing-worker-fatal fold-in). A
	// non-cancellable writer may outlive the bound; the engine's published
	// fatal is still folded in without waiting on Run.
	DrainTimeout time.Duration
}

// Metrics holds engine counters (POC observability: expvar-style atomics).
// CommittedCount counts messages committed this engine instance; CheckpointPtr
// mirrors the durable checkpoint position (M2 review R5 split them apart).
type Metrics struct {
	MessagesIn     atomic.Int64
	CommittedCount atomic.Int64
	CheckpointPtr  atomic.Int64
	DeadLettered   atomic.Int64
	CelEvalErrors  atomic.Int64
	NoMatch        atomic.Int64
	Retries        atomic.Int64
	DlqFailures    atomic.Int64
	OptionalDrops  atomic.Int64
	DecodeErrors   atomic.Int64
	TransformRuns  atomic.Int64
	Backpressured  atomic.Int64
	SpoolFailures  atomic.Int64
}

// Store is the persistence surface the engine depends on (candidate 04
// facet split): the spool/checkpoint/source-state facet plus dead letters.
// Job run history is deliberately not part of the engine's contract — the
// engine neither reads nor writes run records.
type Store interface {
	store.SpoolStore
	store.DeadLetterStore
}

// Engine runs one pipeline against one store.
type Engine struct {
	IR      *ir.Pipeline
	Store   Store
	Reg     *registry.Registry
	Opts    Options
	Metrics Metrics

	commit     *commitTracker
	chans      map[string]chan *instance
	sinks      map[string]registry.Sink
	codecs     map[string]registry.Codec // resolved by codec name
	// codecMu guards the lazy codecs cache: source goroutines (entry
	// decode), sink workers (encode) and operator replays/injections
	// (dispatchInternal) resolve concurrently, and a foreign codec name — a
	// replayed row whose codec the running config no longer declares — can
	// be un-cached (adversarial review 2026-09-24: an unguarded map write
	// here was a concurrent-map-write panic).
	codecMu sync.Mutex
	sources    map[string]registry.Source
	transforms map[string]registry.Transform

	// admit is the inbound path (gate, ledger, stamping, spool, dispatch);
	// committers are the per-source async watermark writers (Run starts them
	// before the workers and flushes them after drain).
	admit      *admission
	committers map[string]*sourceCommitter

	// replayDone flips once Run's crash replay has registered everything:
	// WaitCommit must not read "committed" while it is in flight (an
	// unregistered replay row momentarily looks like outstanding==0).
	replayDone atomic.Bool

	spanMu sync.Mutex
	spans  map[int64]trace.Span // spool seq → sampled per-message span (nil rate = empty)

	persistMu        sync.Mutex
	persistedThrough int64 // highest checkpoint successfully written
	flushAttempted   int64 // highest advance whose persistence was attempted
	retentionDue     int64 // persistedThrough that triggers the next spool trim

	srcWG    sync.WaitGroup // live source goroutines (exhaustion tracking)
	srcErrMu sync.Mutex
	srcErr   map[string]error // first error per pull source
	srcDone  map[string]bool  // sources that returned (exhausted or failed)
	srcTotal int
	srcStart atomic.Bool // Run counted the sources; SourcesDone is meaningful

	fatalMu  sync.Mutex
	fatalErr error // first worker-fatal error (transform clone failure); stops the engine

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// runCalled guards against a second Run (CAS first, before Run mutates
	// anything); started is the readiness publication and is set only AFTER
	// ctx/cancel are assigned, so injectAt's started gate (and Ready()) also
	// observes the live context — CAS on started alone would race the ctx
	// assignment (review follow-up, -race repro on TestEngineInjectReplay).
	runCalled atomic.Bool
	started   atomic.Bool
}

// instance is one in-flight message positioned at a node.
type instance struct {
	seq  int64 // spool sequence
	msg  registry.Message
	via  *ir.Edge // edge it arrived on (nil at entry)
	node string
}

// New builds an engine: resolves plugins and codecs, allocates channels and
// the commit tracker. Call Run to start it. Options are normalized through
// the one withDefaults path.
func New(p *ir.Pipeline, st Store, reg *registry.Registry, opts Options) (*Engine, error) {
	opts = opts.withDefaults()
	if opts.DLQRetention <= 0 && p.Config != nil && p.Config.DLQ != nil {
		// No non-zero default here by design: unset dlq.retention keeps dead
		// letters forever (they are `replay` input, not garbage).
		opts.DLQRetention = p.Config.DLQ.Retention
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
		transforms: map[string]registry.Transform{},
		committers: map[string]*sourceCommitter{},
		srcErr:     map[string]error{},
		srcDone:    map[string]bool{},
		spans:      map[int64]trace.Span{},
	}

	for _, name := range p.Order {
		n := p.Nodes[name]
		switch n.Section {
		case config.SectionSource:
			var src registry.Source
			var err error
			if n.Config.Grpc != nil {
				src, err = rpcplugin.SpawnSource(context.Background(), n.Config.Grpc, n.Config.Manifest, pluginCfgMap(n.Config), opts.Logf,
					rpcplugin.WithRestartCounter(opts.Obs.RecordPluginRestart))
			} else {
				src, err = reg.NewSource(n.Config.Plugin, pluginCfgMap(n.Config))
			}
			if err != nil {
				return nil, fmt.Errorf("source %q: %w", name, err)
			}
			e.sources[name] = src
			// The loader materialized the decoder (candidate 06).
			if _, err := e.codec(n.Config.Decoder, reg); err != nil {
				return nil, fmt.Errorf("source %q: %w", name, err)
			}
		case config.SectionSink:
			var sink registry.Sink
			var err error
			if n.Config.Grpc != nil {
				sink, err = rpcplugin.SpawnSink(context.Background(), n.Config.Grpc, n.Config.Manifest, pluginCfgMap(n.Config), opts.Logf,
					rpcplugin.WithRestartCounter(opts.Obs.RecordPluginRestart))
			} else {
				sink, err = reg.NewSink(n.Config.Plugin, pluginCfgMap(n.Config))
			}
			if err != nil {
				return nil, fmt.Errorf("sink %q: %w", name, err)
			}
			if opts.SinkWrapper != nil {
				sink = opts.SinkWrapper(name, sink)
			}
			e.sinks[name] = sink
			// The loader materialized the encoder (candidate 06).
			if _, err := e.codec(n.Config.Encoder, reg); err != nil {
				return nil, fmt.Errorf("sink %q: %w", name, err)
			}
			e.chans[name] = make(chan *instance, channelCapacity(opts.ChannelSize, n.In))
		case config.SectionTransform:
			// Transforms instantiate through the registry like sources and
			// sinks (spec v1.19); Init hands the plugin its constants,
			// parameters, node logger and the slow-call advisory threshold
			// before workers start.
			t, err := reg.NewTransform(n.Config.Plugin, n.Config.PluginConfig, p.Config.BaseDir)
			if err != nil {
				return nil, fmt.Errorf("transform %q: %w", name, err)
			}
			nodeLogf := func(format string, args ...any) {
				opts.Logf("[node %s] "+format, append([]any{name}, args...)...)
			}
			if err := t.Init(&registry.TransformEnv{
				Constants:    p.Constants,
				Parameters:   p.Parameters,
				Logf:         nodeLogf,
				SlowCallWarn: time.Duration(opts.WasmSlowCallWarnMs) * time.Millisecond,
			}); err != nil {
				_ = t.Close()
				return nil, fmt.Errorf("transform %q: %w", name, err)
			}
			e.transforms[name] = t
			e.chans[name] = make(chan *instance, channelCapacity(opts.ChannelSize, n.In))
		}
	}

	var sourceNames []string
	for _, name := range p.Order {
		if p.Nodes[name].Section == config.SectionSource {
			sourceNames = append(sourceNames, name)
		}
	}
	e.commit = newCommitTracker(p.Config.Name, sourceNames, e.onCommit, e.persistCheckpoint)
	e.admit = newAdmission(e)
	for name, src := range e.sources {
		e.committers[name] = newSourceCommitter(p.Config.Name, name, src, st, opts.DrainTimeout)
	}
	return e, nil
}

// channelCapacity sizes a node's dispatch channel: the default ChannelSize
// raised to the largest inbound edge buffer. A per-edge BufferMax acts as
// surge capacity (§6.2), so it only participates while it stays below 4x the
// running capacity — an outlier edge must not dominate memory sizing. Shared
// by sink and transform channels so both follow one rule (review-2026-09).
func channelCapacity(base int, in []ir.Edge) int {
	capacity := base
	for _, edge := range in {
		if edge.BufferMax > 0 && edge.BufferMax < capacity*4 {
			capacity = maxInt(capacity, edge.BufferMax)
		}
	}
	return capacity
}

// onCommit releases the backpressure slot of one committed message, counts
// it (R5: a count, distinct from the checkpoint pointer) and records its
// accept-to-commit latency.
func (e *Engine) onCommit(seq int64) {
	e.Metrics.CommittedCount.Add(1)
	e.finishSpan(seq, "committed", "")
	accepted := e.admit.takeAccepted(seq)
	latency := time.Duration(0)
	if !accepted.IsZero() {
		latency = e.Opts.Clock().Sub(accepted)
	}
	e.Opts.Obs.RecordCommit(e.IR.Config.Name, latency)
	e.admit.release(seq)
}

// persistCheckpoint advances the durable checkpoint (invariant 2) and posts
// the per-source frontiers to their committers. It runs OUTSIDE the commit
// tracker's lock, on the goroutine that committed the prefix; concurrent
// advances can therefore flush out of order, so monotonic guards (persistMu)
// keep the checkpoint and the attempt pointer from ever regressing. A failed
// checkpoint write only widens the replay window on crash; the next advance
// retries. Never a loss (invariant 3).
//
// Source watermarks are persisted asynchronously (sourceCommitter): the
// durable barrier is the checkpoint, unchanged, and a source state that lags
// it only widens the replay window on crash — duplicate delivery, never loss.
func (e *Engine) persistCheckpoint(committedThrough int64, frontiers map[string]int64) {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	if committedThrough > e.persistedThrough {
		if err := e.Store.SetCheckpoint(e.IR.Config.Name, committedThrough); err != nil {
			// Consume the flush position anyway: observers waiting on the
			// visibility barrier must not block on a store that keeps
			// failing — durability is retried by the next advance.
			e.flushAttempted = committedThrough
			return
		}
		e.persistedThrough = committedThrough
		e.Metrics.CheckpointPtr.Store(committedThrough)
	}
	if committedThrough > e.flushAttempted {
		e.flushAttempted = committedThrough
	}
	// Spool retention: one batched trim per window of durable progress, not
	// per advance — piggybacking here keeps the delete rate off the message
	// path entirely. The cutoff is derived from persistedThrough (NOT
	// committedThrough): while checkpoint writes fail, the durable position
	// lags the in-memory one, and trimming ahead of what a restart would
	// replay from would turn at-least-once into loss. A failed trim is
	// logged and retried by the next window (the sweep range only grows).
	if pt := e.persistedThrough; pt >= e.retentionDue {
		if cutoff := pt - e.Opts.SpoolRetention; cutoff > 0 {
			if _, err := e.Store.DeleteSpoolThrough(e.IR.Config.Name, cutoff); err != nil && e.Opts.Logf != nil {
				e.Opts.Logf("engine: spool retention: %v", err)
			}
		}
		// DLQ retention rides the same window (opt-in; a no-op unless
		// dlq.retention is configured). Dead letters are terminal artifacts:
		// trimming them cannot affect the invariants — but the rows are gone
		// from `replay` for good, which is why the sweep never runs unset.
		e.trimDeadLetters()
		e.retentionDue = pt + e.Opts.SpoolRetention
	}
	for name, frontier := range frontiers {
		if c, ok := e.committers[name]; ok {
			c.post(frontier)
		}
	}
}

// trimDeadLetters prunes dead letters older than the configured retention
// (dlq.retention, §5.10). OPT-IN: retention 0 (unset) means keep forever —
// dead letters are operator data for `replay`, and automatic deletion would
// silently destroy it. The cutoff rides the engine clock so tests can move
// time deterministically. Errors only log and retry on the next retention
// window (same contract as the spool sweep): the commit path must never
// block on a store that fails to prune history.
func (e *Engine) trimDeadLetters() {
	if e.Opts.DLQRetention <= 0 {
		return
	}
	cutoff := e.Opts.Clock().Add(-e.Opts.DLQRetention)
	n, err := e.Store.DeleteDeadLettersBefore(e.IR.Config.Name, cutoff)
	if err != nil {
		if e.Opts.Logf != nil {
			e.Opts.Logf("engine: dlq retention: %v", err)
		}
		return
	}
	if n > 0 && e.Opts.Logf != nil {
		e.Opts.Logf("engine: dlq retention: removed %d dead letter(s) older than %s", n, cutoff.UTC().Format(time.RFC3339))
	}
}

// durableThrough reports the highest commit advance whose persistence has
// been ATTEMPTED (successfully or not). It is the visibility barrier: snapshot
// accounting (committedThrough) may lead it only while an inline flush is in
// flight — never after the committing goroutine has moved on.
func (e *Engine) durableThrough() int64 {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	return e.flushAttempted
}

// codec resolves a codec by name: named `codecs:` declarations come
// pre-instantiated on the IR (config validated at verify); bare names
// instantiate through the registry (no config, no relative paths).
func (e *Engine) codec(name string, reg *registry.Registry) (registry.Codec, error) {
	e.codecMu.Lock()
	defer e.codecMu.Unlock()
	if c, ok := e.codecs[name]; ok {
		return c, nil
	}
	if e.IR != nil {
		if c, ok := e.IR.Codecs[name]; ok {
			e.codecs[name] = c
			return c, nil
		}
	}
	c, err := reg.NewCodec(name, nil, "")
	if err != nil {
		return nil, err
	}
	e.codecs[name] = c
	return c, nil
}

// Run starts the engine and blocks until ctx is done. The phases are ordered
// by an invariant (candidate 01): source committers and workers first, then
// crash replay, then sources. Replay dispatches into node channels, so the
// consumers must exist before it runs — a recovery with more uncommitted rows
// than a channel holds used to block forever on a channel nobody read. A
// second call on the same Engine is a programming error (it would replay the
// spool and duplicate workers) and returns an error instead (review-2026-09).
func (e *Engine) Run(ctx context.Context) error {
	if !e.runCalled.CompareAndSwap(false, true) {
		return errors.New("engine: Run called twice")
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	// Readiness publication comes after the ctx assignment: injectAt and
	// Ready() gate on this flag, so it must never be observable before the
	// context it promises is live.
	e.started.Store(true)

	// Source committers first: the first worker commit must have somewhere
	// to post its frontier.
	for _, c := range e.committers {
		go c.run(e.ctx)
	}

	e.startWorkers()
	if err := e.replaySpool(); err != nil {
		e.shutdown()
		return err
	}
	e.startSources()

	<-e.ctx.Done()
	e.shutdown()
	e.fatalMu.Lock()
	defer e.fatalMu.Unlock()
	return e.fatalErr
}

// startWorkers launches the transform and sink goroutines (Run phase 1).
func (e *Engine) startWorkers() {
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
}

// replaySpool re-dispatches every spool row beyond the checkpoint (invariant
// 3, Run phase 2). A cancelled context is a voluntary stop (v1.24): the scan
// stops and Run continues to start the sources, never surfacing an error.
func (e *Engine) replaySpool() error {
	cp, err := e.Store.Checkpoint(e.IR.Config.Name)
	if err != nil {
		return fmt.Errorf("engine: read checkpoint: %w", err)
	}
	err = e.Store.ReplayFrom(e.IR.Config.Name, cp, func(seq int64, msg registry.Message, ingestTime time.Time) error {
		node, intoNode, ok := e.replayEntry(msg)
		if !ok {
			// Spooled but never dispatched and not attributable: release it
			// instead of wedging the contiguous prefix.
			e.commit.arrived(seq, "", 0)
			e.commit.done(seq)
			return nil
		}
		_, aerr := e.admit.admit(e.ctx, admitRequest{
			mode:     admitReplay,
			node:     node,
			msg:      msg,
			seq:      seq,
			ingest:   ingestTime,
			intoNode: intoNode,
		})
		if aerr == nil {
			return nil
		}
		if e.ctx.Err() != nil {
			return errReplayStopped // cancelled: stop replaying, not a failure
		}
		return aerr
	})
	if err != nil && !errors.Is(err, errReplayStopped) {
		return fmt.Errorf("engine: replay: %w", err)
	}
	e.replayDone.Store(true)
	return nil
}

// errReplayStopped aborts the ReplayFrom scan without failing Run: the
// engine context was cancelled mid-replay (voluntary stop, v1.24).
var errReplayStopped = errors.New("engine: replay stopped")

// startSources initializes and launches the source goroutines (Run phase 3).
// Pull sources (job pipelines) use Pull and signal exhaustion or failure; the
// job runner watches SourcesDone/SourceErrors for run completion (M2 review
// R1).
func (e *Engine) startSources() {
	if e.Opts.DisableSources {
		e.srcErrMu.Lock()
		e.srcTotal = len(e.sources)
		for name := range e.sources {
			e.srcDone[name] = true
		}
		e.srcStart.Store(true)
		e.srcErrMu.Unlock()
		return
	}
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
			// Both source kinds report completion identically (v1.24):
			// a nil return is exhaustion/voluntary stop, an error is a
			// failed source. emit's error is the admission verdict
			// (candidate 01): a refusal is returned by the builtin default,
			// so it becomes a failed source here.
			var err error
			if ps, ok := src.(registry.PullSource); ok {
				err = ps.Pull(e.ctx, func(msg registry.Message) error {
					return e.accept(msg, name)
				})
			} else {
				err = src.Run(e.ctx, func(msg registry.Message) error {
					return e.accept(msg, name)
				})
			}
			// A genuine source failure stops the engine in every mode
			// (candidate 02): a continuous pipeline must not keep running
			// with a dead source, and restart resumes from the source
			// watermarks (duplicate delivery, never loss). The error is
			// recorded BEFORE the cancel so any observer of the stop (and
			// the Wait classification) sees it. An error returned under an
			// already-cancelled engine ctx is a shutdown artifact, not a
			// failure: the emit contract defines ctx cancellation as a
			// voluntary stop (the source returns nil, never ctx.Err()).
			if err != nil && e.ctx.Err() == nil {
				e.markSourceDone(name, err)
				e.cancel()
			} else {
				e.markSourceDone(name, nil)
			}
			_ = src.Close()
		}(name, src)
	}
}

// shutdown drains the workers and sources, stops the source committers and
// flushes their pending frontiers synchronously with a bounded,
// non-cancelled context (the engine ctx is already cancelled; a cancelled
// Commit would discard the last frontier).
func (e *Engine) shutdown() {
	e.cancel()
	e.drain()
	for _, c := range e.committers {
		c.wait(e.Opts.DrainTimeout)
	}
	for _, c := range e.committers {
		c.flush()
	}
}

// failNode records a worker-fatal error and stops the engine: per-message
// failures dead-letter, but a node that cannot run at all (a transform whose
// Clone fails — the master instance is not goroutine-safe, the very reason
// TransformCloner exists) must not degrade into sharing it across workers.
// The first error wins; anything already in flight stays uncommitted and is
// replayed on restart (invariant 3). Run reports the error after draining.
func (e *Engine) failNode(node string, err error) {
	e.fatalMu.Lock()
	if e.fatalErr == nil {
		e.fatalErr = fmt.Errorf("node %q: %w", node, err)
	}
	e.fatalMu.Unlock()
	e.cancel()
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
		switch e.IR.Nodes[name].Section {
		case config.SectionSink:
			if s, ok := e.sinks[name]; ok {
				_ = s.Close()
			}
		case config.SectionTransform:
			// Master instances (worker clones close themselves in
			// runTransform); the wasm template releases the wazero runtime.
			if t, ok := e.transforms[name]; ok {
				_ = t.Close()
			}
		}
	}
}

// Close cancels the engine (idempotent). The started gate publishes the
// ctx/cancel assignment from Run: reading cancel only after started is true
// makes Close safe to call from a goroutine racing Run's startup.
func (e *Engine) Close() {
	if e.started.Load() {
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
// completion detection: sources exhausted AND commit outstanding == 0).
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
// all sources stopped, nothing uncommitted, and every commit advance flushed
// (attempted) — job runners poll this to move a run into its terminal state.
// The replayDone/admitting guards close two holes where a message in flight
// looks like no work at all: the crash-replay scan has not registered its
// rows yet, or an admission holds a gate slot but has not called arrived
// (candidate 02).
func (e *Engine) Quiesced() bool {
	if !e.SourcesDone() {
		return false
	}
	if !e.replayDone.Load() || e.admit.admitting.Load() != 0 {
		return false
	}
	outstanding, committedThrough, _ := e.commit.snapshot()
	return outstanding == 0 && e.durableThrough() >= committedThrough
}

// WaitQuiesced blocks until the pipeline is quiesced — the completion point
// of a job or batch run (v1.24). runDone is eng.Run's result channel: an
// engine stop before quiescence (worker-fatal, source failure) is surfaced as
// the returned error, a nil result as "engine stopped before quiescence". ctx
// cancellation returns ctx.Err(); when ctx and runDone fire together the
// CONSUMER must re-check ctx.Err() first so a canceled run is never reported
// as a failure. Engine.Wait builds on the same loop; this stays the
// low-level primitive (tests use it).
func (e *Engine) WaitQuiesced(ctx context.Context, runDone <-chan error) error {
	waitErr, _, _ := e.waitQuiesced(ctx, runDone)
	return waitErr
}

// waitQuiesced is the shared wait loop behind WaitQuiesced and Wait: it
// returns when the pipeline is quiesced (nil, not returned), when ctx is done
// (ctx.Err(), not returned), or when Run returns (waitErr, runErr, returned —
// runErr is Run's raw result, nil included).
func (e *Engine) waitQuiesced(ctx context.Context, runDone <-chan error) (waitErr, runErr error, runReturned bool) {
	for {
		if e.Quiesced() {
			return nil, nil, false
		}
		select {
		case <-ctx.Done():
			return ctx.Err(), nil, false
		case err := <-runDone:
			if err == nil {
				return errors.New("engine stopped before quiescence"), nil, true
			}
			return err, err, true
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Wait drives one run to its terminal state and classifies it: quiesce →
// cancel → bounded wait for Run → Outcome (candidate 02). Every runner maps
// this one result onto its own process contract; none of them re-derives the
// terminal state. Classification priority: worker-fatal → source errors →
// caller cancellation (the ctx passed in, never the engine's internal cancel,
// so an engine self-stop is never misreported as interrupted) → dead letters
// > 0 (partial) → completed. On caller cancellation with AbandonOnCancel the
// outstanding set is terminal-dead-lettered first (write-first, bounded).
func (e *Engine) Wait(ctx context.Context, runDone <-chan error, opts WaitOptions) Outcome {
	_, runErr, runReturned := e.waitQuiesced(ctx, runDone)
	callerCanceled := ctx.Err() != nil

	abandoned, abandonErr := 0, error(nil)
	if callerCanceled && opts.AbandonOnCancel {
		timeout := opts.AbandonTimeout
		if timeout <= 0 {
			timeout = e.Opts.DrainTimeout
		}
		// The caller's ctx is already done, so it cannot bound the abandon:
		// derive a fresh context that keeps the values but not the
		// cancellation (the bound is the abandon's, not the shutdown's).
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		abandoned, abandonErr = e.Abandon(actx, abandonReason(opts.AbandonReason))
		cancel()
	}

	if !runReturned {
		e.Close() // idempotent; nil-safe if Run has not assigned its cancel yet
		bound := opts.DrainTimeout
		if bound <= 0 {
			bound = e.Opts.DrainTimeout + 5*time.Second
		}
		timer := time.NewTimer(bound)
		defer timer.Stop()
		select {
		case runErr = <-runDone:
		case <-timer.C:
			// The engine outlived the bound (a non-cancellable writer may
			// outlive the run by design, R2). Stop it and fold in its
			// published fatal instead of waiting on Run: failNode records
			// the error before it cancels, so a racing worker-fatal is
			// never dropped.
			e.Close()
			e.fatalMu.Lock()
			runErr = e.fatalErr
			e.fatalMu.Unlock()
		}
	}

	sourceErrors := e.SourceErrors()
	deadLettered := e.Metrics.DeadLettered.Load()
	return Outcome{
		Status:       classifyOutcome(runErr, sourceErrors, callerCanceled, deadLettered),
		RowsRead:     e.Metrics.MessagesIn.Load(),
		Committed:    e.Metrics.CommittedCount.Load(),
		DeadLettered: deadLettered,
		SourceErrors: sourceErrors,
		WorkerFatal:  runErr,
		Abandoned:    abandoned,
		AbandonError: abandonErr,
	}
}

// classifyOutcome applies the terminal priority (candidate 02): worker-fatal
// → source errors → caller cancellation → dead letters > 0 (partial) →
// completed.
func classifyOutcome(workerFatal error, sourceErrors map[string]error, callerCanceled bool, deadLettered int64) RunStatus {
	switch {
	case workerFatal != nil:
		return RunFailed
	case len(sourceErrors) > 0:
		return RunFailed
	case callerCanceled:
		return RunInterrupted
	case deadLettered > 0:
		return RunPartial
	default:
		return RunCompleted
	}
}

// abandonReason defaults the dead-letter reason of the abandon path.
func abandonReason(reason string) string {
	if reason == "" {
		return "run canceled"
	}
	return reason
}

// Abandon terminal-dead-letters every outstanding message (M2 review R2: a
// canceled run must not leave uncommitted spool rows wedging the checkpoint's
// contiguous prefix forever). It is BOUNDED by the caller's ctx and the
// ordering preserves the invariants: the dead letter is written FIRST, and
// only after a successful write does the tracker clear the message (any
// leftover branches from a mid-flight fan-out are force-terminated after the
// durable record exists). A ctx cancellation or store failure stops the
// attempt and returns an error, leaving every message it did not record
// uncommitted — the next run replays them (never loss; CONTEXT.md "Abandon").
// It returns the number abandoned plus the first error.
func (e *Engine) Abandon(ctx context.Context, reason string) (int, error) {
	abandoned := 0
	_, committedThrough, _ := e.commit.snapshot()
	after := committedThrough
	for {
		if err := ctx.Err(); err != nil {
			return abandoned, fmt.Errorf("engine: abandon: %w", err)
		}
		var seqs []int64
		msgs := map[int64]registry.Message{}
		last, more, ferr := e.Store.ReplayPage(e.IR.Config.Name, after, 256,
			func(seq int64, msg registry.Message, _ time.Time) error {
				if e.commit.isOutstanding(seq) {
					seqs = append(seqs, seq)
					msgs[seq] = msg
				}
				return nil
			})
		if ferr != nil {
			return abandoned, fmt.Errorf("engine: abandon: %w", ferr)
		}
		for _, seq := range seqs {
			if err := ctx.Err(); err != nil {
				return abandoned, fmt.Errorf("engine: abandon: %w", err)
			}
			msg := msgs[seq]
			dl := e.deadLetterRecord(msg, firstNonEmpty(msg.SrcName, "unknown"), "", store.DLClassCanceled, reason, "")
			if err := e.writeDeadLetter(ctx, seq, dl); err != nil {
				return abandoned, fmt.Errorf("engine: abandon: %w", err)
			}
			if e.commit.forceTerminal(seq) {
				// The force-terminate removes the entry without a terminal
				// branch event, so the commit sweep never fires onCommit for
				// it — the admission slot would leak (candidate 01). release
				// is idempotent, so a racing commit sweep cannot double-free.
				e.admit.release(seq)
			}
			abandoned++
		}
		if !more || last == after {
			break
		}
		after = last
	}
	return abandoned, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// accept is the thin live shell over admission: one source emission through
// the gate, stamping, the durable spool append and the dispatch (invariant 1:
// nothing is visible before the append succeeds). The returned error is the
// admission verdict the emit contract exposes (nil = durably accepted, ctx
// error = shutdown, anything else = refusal).
func (e *Engine) accept(raw registry.Message, sourceNode string) error {
	_, err := e.admit.admit(e.ctx, admitRequest{mode: admitLive, node: sourceNode, msg: raw})
	return err
}

// startMessageSpan samples and starts one per-message span
// (telemetry.span_sample_rate > 0 only; R16: sampling is opt-in). The span
// context is deliberately not propagated through the DAG — correlation rides
// the attributes (pipeline/message_id/source).
func (e *Engine) startMessageSpan(seq int64, messageID, source string) {
	rate := e.Opts.SpanSampleRate
	if rate <= 0 {
		return
	}
	if rate < 1 && rand.Float64() >= rate {
		return
	}
	_, span := e.Opts.Obs.Tracer().Start(e.ctx, "eventboat.message",
		trace.WithAttributes(
			attribute.String("eventboat.pipeline", e.IR.Config.Name),
			attribute.String("eventboat.message_id", messageID),
			attribute.String("eventboat.source", source),
			attribute.Int64("eventboat.spool_seq", seq),
		))
	e.spanMu.Lock()
	e.spans[seq] = span
	e.spanMu.Unlock()
}

// finishSpan ends one sampled span at its terminal state; unknown seqs
// (unsampled, or ended by a racing terminal path) are no-ops.
func (e *Engine) finishSpan(seq int64, terminal, errText string) {
	e.spanMu.Lock()
	span := e.spans[seq]
	delete(e.spans, seq)
	e.spanMu.Unlock()
	if span == nil {
		return
	}
	span.SetAttributes(attribute.String("eventboat.terminal_state", terminal))
	if errText != "" {
		span.SetAttributes(attribute.String("eventboat.error", errText))
	}
	span.End()
}

// dispatchFrom decodes (at source entry) and fans a spooled message out of
// its originating node.
func (e *Engine) dispatchFrom(sourceNode string, seq int64, msg registry.Message) {
	node := e.IR.Nodes[sourceNode]
	codec, err := e.codec(msg.Codec, e.Reg)
	if err != nil {
		e.deadLetterMsg(seq, msg, sourceNode, "", store.DLClassCodec, "codec: "+err.Error(), "")
		return
	}
	if msg.Decoded == nil {
		v, derr := codec.Decode(msg.Raw)
		if derr != nil {
			e.Metrics.DecodeErrors.Add(1)
			e.Opts.Obs.RecordDecodeError(e.IR.Config.Name, sourceNode)
			e.deadLetterMsg(seq, msg, sourceNode, "", store.DLClassDecode, "decode: "+derr.Error(), "")
			return
		}
		msg.Decoded = v
	}
	e.fanOut(node, seq, msg)
}

// fanOut evaluates outgoing edge conditions (CEL errors count and act as
// not-passed) and delivers to every matched edge. Zero matches commit the
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
		e.commit.done(seq)
		return
	}
	e.commit.add(seq, len(matched)-1)
	for _, edge := range matched {
		e.deliver(edge, seq, msg)
	}
}

func (e *Engine) deliver(edge *ir.Edge, seq int64, msg registry.Message) {
	inst := &instance{seq: seq, msg: msg, via: edge, node: edge.To}
	select {
	case e.chans[edge.To] <- inst:
	case <-e.ctx.Done():
		// Shutdown with undelivered work: deliberately NOT committed, it stays
		// uncommitted and will be replayed from the spool on restart
		// (invariant 3: replay covers the uncommitted set).
	}
}

// InjectAt feeds a message into a node: at a source it is spooled and fans
// out of the source (there is no source logic to run); at an internal node it
// is spooled and enters the DAG at that node (testkit / replay). The message
// carries its own Raw/Meta/Codec/ID; a non-empty ID is preserved, and an
// empty codec defaults to json (callers injecting at a source pass the node's
// decoder when they have no codec of their own — internal/testrun does).
func (e *Engine) InjectAt(node string, msg registry.Message) (int64, error) {
	return e.injectAt(node, msg)
}

// InjectReplay re-injects one previously dead-lettered (or spooled) message
// (§3.3): it enters at the given node, keeps its ORIGINAL message_id (so
// idempotent sinks deduplicate re-deliveries) and is stamped
// meta.is_replay=true for sinks to recognize. The message's Codec travels
// with it — a csv dead letter replays as csv, never as json (candidate 01).
func (e *Engine) InjectReplay(node string, msg registry.Message) (int64, error) {
	meta := cloneMeta(msg.Meta)
	meta["is_replay"] = true
	if msg.ID != "" {
		meta["original_message_id"] = msg.ID
	}
	msg.Meta = meta
	return e.injectAt(node, msg)
}

// dispatchInternal delivers a message INTO a node instead of fanning out of
// it: it decodes with the message's codec (so sink encoders can re-encode;
// Raw is the spooled truth and stays untouched) and hands the node its own
// entry — transforms run their script (a replay after a script fix
// re-executes it), sinks batch and write under their delivery policy (M2
// review R4: replay can target any node). Live InjectAt and Run's crash
// replay of injected rows share this path so both replay semantics match
// (review-2026-09). The single outstanding branch registered by arrived is
// exactly this one delivery.
func (e *Engine) dispatchInternal(node string, seq int64, msg registry.Message) {
	codec, err := e.codec(msg.Codec, e.Reg)
	if err != nil {
		e.deadLetterMsg(seq, msg, node, "", store.DLClassCodec, "codec: "+err.Error(), "")
		return
	}
	if msg.Decoded == nil {
		v, derr := codec.Decode(msg.Raw)
		if derr != nil {
			e.Metrics.DecodeErrors.Add(1)
			e.Opts.Obs.RecordDecodeError(e.IR.Config.Name, node)
			e.deadLetterMsg(seq, msg, node, "", store.DLClassDecode, "decode: "+derr.Error(), "")
			return
		}
		msg.Decoded = v
	}
	// An injected message carries no operator-configured edge. It is
	// delivered as a REQUIRED synthetic edge: a re-failure must dead-letter
	// again, never take the optional-drop path — `replay --delete` removes
	// the original record once the reinjection commits, so a silent drop
	// here would lose the message (adversarial review 2026-09-24).
	e.deliver(&ir.Edge{From: node, To: node, Required: true}, seq, msg)
}

// WaitCommit blocks until no outstanding branches remain and every commit
// advance has been flushed (attempted) — commit completion must still imply
// persistence visibility now that the flush runs outside the tracker lock
// (test helper; the durability barrier keeps that implication explicit).
func (e *Engine) WaitCommit(ctx context.Context) error {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		outstanding, committedThrough, _ := e.commit.snapshot()
		if outstanding == 0 && e.durableThrough() >= committedThrough && e.replayDone.Load() && e.admit.admitting.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			outstanding, committed, arrived := e.commit.snapshot()
			return fmt.Errorf("wait commit: ctx done with %d outstanding (committedThrough=%d arrivedMax=%d durableThrough=%d)", outstanding, committed, arrived, e.durableThrough())
		case <-tick.C:
		}
	}
}

// CommitSnapshot exposes commit counters for tests and status.
func (e *Engine) CommitSnapshot() (outstanding int, committedThrough int64, arrivedMax int64) {
	return e.commit.snapshot()
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
