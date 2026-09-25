// Package ops is the operations service: the single implementation behind
// the MCP tools and the Admin REST endpoints (redesign-v3.md §3.4). Every
// write path goes through verify-first; there is no bypass channel.
package ops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/dlq"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/jobs"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testrun"
	"github.com/eventboat/eventboat/internal/verify"
)

// Options configures the service.
type Options struct {
	DataDir string // deployed pipeline files live under <DataDir>/pipelines
	Reg     *registry.Registry
	// Stores is the process-wide store provider (candidate 04): the entry
	// point builds one owner (store.NewOwner / store.NewMemoryOwner) and
	// passes it here, so every surface shares one handle per pipeline. It
	// also hands out each pipeline's cross-process lease (candidate 08):
	// ops acquires it before starting an instance and refuses a Deploy when
	// another process holds it. There is no default factory — layout and
	// handle lifetime belong to the owner.
	Stores store.Provider
	// SpoolRetention bounds spool rows behind the checkpoint
	// (storage.spool_retention; 0 = the engine default) — passed through to
	// every managed engine.
	SpoolRetention int64
	// Clock for rate windows (tests).
	Clock func() time.Time
	// Obs receives telemetry (nil disables).
	Obs *obs.Obs
}

// Service manages the running pipelines of one Eventboat process.
type Service struct {
	opts Options
	reg  *registry.Registry

	mu         sync.Mutex
	pipelines  map[string]*managed
	lifecycles map[string]*sync.Mutex // per-pipeline lifecycle serialization

	tailMu sync.Mutex
	tails  map[string][]TailEntry // node → recent deliveries (bounded)

	rateMu   sync.Mutex
	lastSnap map[string]int64 // pipeline → messages_in at last snapshot (rate deltas)
	rateAt   time.Time

	// counterMu guards lastCounters: the baseline of every polled source
	// counter, keyed "pipeline\x00node\x00counter". Status writes the delta
	// since the previous observation to telemetry and never a negative one.
	counterMu    sync.Mutex
	lastCounters map[string]int64

	subMu sync.Mutex
	subs  map[chan Event]struct{}

	// stopped flips when Stop begins, so /ready answers 503 while the
	// service drains instead of leaving a terminating pod in rotation.
	stopped atomic.Bool
}

// managed is one deployed pipeline: either a continuous engine or a job
// manager (per its run.mode). Its status moves through the instance state
// machine below; its lease is the cross-process single-writer lock on the
// pipeline's store (candidate 08).
type managed struct {
	name    string
	file    string
	cfg     *config.Pipeline
	pip     *ir.Pipeline // built IR of a continuous/batch instance (nil for job)
	kind    string       // "continuous" | "job" | "batch"
	eng     *engine.Engine
	jobs    *jobs.Manager
	cancel  context.CancelFunc
	done    chan struct{}
	started time.Time

	lease     store.Lease
	leaseOnce sync.Once

	mu     sync.Mutex // guards status/err (written from lifecycle goroutines)
	status string
	err    string
}

// Instance statuses (candidate 08, §3.7): the daemon's per-pipeline
// lifecycle. `completed` and `failed` are terminal for one instance — a
// Deploy replaces the instance; Pause/Drain/Resume refuse instead of
// reporting a state the pipeline is not in.
const (
	stateRunning   = "running"
	statePaused    = "paused"
	stateDrained   = "drained"
	stateCompleted = "completed"
	stateFailed    = "failed"
)

// instanceTransitions is the explicit transition table: running may be
// paused, drained, or finish (completed/failed); paused and drained restart
// via Resume. Terminal states have no outgoing edges. Repeating the current
// state is an idempotent no-op handled by transition, not listed here.
var instanceTransitions = map[string]map[string]bool{
	stateRunning: {statePaused: true, stateDrained: true, stateCompleted: true, stateFailed: true},
	statePaused:  {stateRunning: true}, // Resume restarts
	stateDrained: {stateRunning: true}, // Resume restarts
}

// state reads the current status.
func (m *managed) state() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// transition applies one state-machine step. Repeating the current state is
// an idempotent no-op (Pause of a paused pipeline, Resume of a running one);
// anything not in the table is refused loudly — no fake `running`.
func (m *managed) transition(to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == to {
		return nil
	}
	if !instanceTransitions[m.status][to] {
		return fmt.Errorf("pipeline %q: %s -> %s is not a valid transition", m.name, m.status, to)
	}
	m.status = to
	return nil
}

// advance moves from -> to only while the instance is still in from: the
// completion watcher's compare-and-set, so a Drain/Pause that already
// stopped the instance is never overwritten by a late "completed".
func (m *managed) advance(from, to string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != from {
		return false
	}
	m.status = to
	return true
}

// setErr is the only writer of the error surface; Status() reads it under
// the same mutex.
func (m *managed) setErr(msg string) {
	m.mu.Lock()
	m.err = msg
	m.mu.Unlock()
}

// releaseLease drops the cross-process store lease once. It is called by the
// instance's own goroutine BEFORE done closes: anyone who observed the stop
// can immediately acquire the lease again (a Deploy replacement) without a
// spurious "held by another process".
func (m *managed) releaseLease() {
	m.leaseOnce.Do(func() {
		if m.lease != nil {
			_ = m.lease.Release()
		}
	})
}

// Event is one SSE-notifiable change.
type Event struct {
	Type string `json:"type"` // status | deploy | job
	Data any    `json:"data"`
}

// TailEntry is one sampled delivery for the tail buffer.
type TailEntry struct {
	Node      string    `json:"node"`
	MessageID string    `json:"message_id"`
	Payload   string    `json:"payload"` // truncated JSON
	At        time.Time `json:"at"`
	IsReplay  bool      `json:"is_replay"`
}

// New builds the service.
func New(opts Options) *Service {
	if opts.Stores == nil {
		// No default factory by ruling (candidate 04): the canonical layout
		// and the handle lifetime belong to one owner, built by the entry
		// point. A missing provider is a programming error, not a runtime
		// condition.
		panic("ops: Options.Stores is required (build a store.Owner or a memory owner)")
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Service{
		opts:       opts,
		reg:        opts.Reg,
		pipelines:  map[string]*managed{},
		lifecycles: map[string]*sync.Mutex{},
		tails:      map[string][]TailEntry{},
		subs:       map[chan Event]struct{}{},
		lastSnap:   map[string]int64{},
		rateAt:     time.Now(),
	}
}

// Subscribe returns a channel of events for SSE streaming.
func (s *Service) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	s.subMu.Lock()
	s.subs[ch] = struct{}{}
	s.subMu.Unlock()
	return ch, func() {
		s.subMu.Lock()
		delete(s.subs, ch)
		s.subMu.Unlock()
	}
}

func (s *Service) emit(typ string, data any) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- Event{Type: typ, Data: data}:
		default: // slow consumers drop events; status snapshots repeat
		}
	}
}

// --- tool implementations ---

// Catalog returns the plugin registry with schemas.
func (s *Service) Catalog() registry.Catalog { return s.reg.Catalog() }

// Verify statically validates a pipeline configuration (content, not path).
// It is the one verify composition (internal/verify): `eventboat verify`,
// the MCP/Admin tools and the LSP all return the same diagnostic sequence
// and strict verdict for the same input. A pure-text submission has no
// filesystem home, so relative paths resolve against the process CWD
// (baseDir "").
func (s *Service) Verify(configContent string) *verify.Result {
	return verify.Bytes("submitted.yaml", []byte(configContent), "", s.reg, verify.Options{})
}

// Test runs a contract suite in-process against its pipeline. Agents pass
// both as text (no shared filesystem); pipelineFile lets local callers
// reference a path instead.
func (s *Service) Test(suiteContent, pipelineContent string) (*testrun.Report, error) {
	dir, err := os.MkdirTemp("", "eventboat-test-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	// The suite sits one level below its root (the documented
	// <root>/tests/<suite>.yaml layout) so the containment check in testrun
	// bounds agent-supplied suite paths to THIS fresh temp dir. The pipeline
	// is written at both spellings' targets: sibling of the suite (the MCP
	// tool contract `pipeline: pipeline.yaml`) and one level up (the
	// README/spec convention `pipeline: ../pipeline.yaml`).
	suiteDir := filepath.Join(dir, "tests")
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return nil, err
	}
	if pipelineContent != "" {
		for _, p := range []string{filepath.Join(suiteDir, "pipeline.yaml"), filepath.Join(dir, "pipeline.yaml")} {
			if err := os.WriteFile(p, []byte(pipelineContent), 0o644); err != nil {
				return nil, err
			}
		}
	}
	suitePath := filepath.Join(suiteDir, "suite.yaml")
	if err := os.WriteFile(suitePath, []byte(suiteContent), 0o644); err != nil {
		return nil, err
	}
	return testrun.RunFile(suitePath, s.reg)
}

// Explain renders the deterministic walkthrough of a configuration through
// the one explain entry (ExplainContent).
func (s *Service) Explain(configContent string, req ExplainRequest) (string, error) {
	return ExplainContent(s.reg, configContent, "", req)
}

// Deploy verifies then swaps one pipeline: verify in memory once, write the
// deployed file, then drain the old instance and start the new one (§3.4 iron
// rule: no write path bypasses verification; no half-deployed state). The
// per-pipeline lifecycle mutex serializes concurrent Deploys, so two of them
// can no longer start two managers on one store.
//
// The deploy directory is the config's base directory for relative paths: the
// deployed file lives there and jobs reload it per run, so wasm/grpc paths
// must resolve the same at deploy time and at run time.
func (s *Service) Deploy(ctx context.Context, configContent string) (map[string]any, error) {
	deployDir := filepath.Join(s.opts.DataDir, "pipelines")
	if err := os.MkdirAll(deployDir, 0o755); err != nil {
		return nil, err
	}
	res := verify.Bytes("submitted.yaml", []byte(configContent), deployDir, s.reg, verify.Options{})
	if res.Pipeline == nil {
		return nil, fmt.Errorf("deploy rejected: verify failed:\n%s", res.Diagnostics.ErrorLines())
	}
	cfg := res.Config

	// Persist the deployed config (jobs reload it per run; restarts re-read it).
	file := filepath.Join(deployDir, cfg.Name+".yaml")
	if err := os.WriteFile(file, []byte(configContent), 0o644); err != nil {
		return nil, err
	}

	lk := s.lifecycle(cfg.Name)
	lk.Lock()
	defer lk.Unlock()
	previous := "none"
	old := s.named(cfg.Name)
	if old != nil {
		previous = old.kind
		if !old.shutdown() {
			return nil, fmt.Errorf("deploy: pipeline %q: the previous instance did not stop; refusing to start a replacement while its runs may still be executing", cfg.Name)
		}
	}
	m, err := s.startManaged(ctx, cfg, res.Pipeline, file)
	if err != nil {
		// The old instance is stopped and no replacement exists: the
		// pipeline is not deployed any more. Drop the entry instead of
		// leaving a stopped instance reporting its previous status.
		s.drop(cfg.Name, old)
		return nil, err
	}
	s.emit("deploy", map[string]any{"pipeline": cfg.Name, "mode": m.kind, "replaced": previous})
	return map[string]any{
		"pipeline": cfg.Name, "mode": m.kind, "replaced": previous,
		"nodes": len(cfg.Order),
	}, nil
}

// lifecycle returns the per-pipeline lifecycle mutex, creating it on first
// use. It outlives any single instance: Deploy/Drain/Pause/Resume serialize
// on it even while the pipeline is being replaced.
func (s *Service) lifecycle(name string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, ok := s.lifecycles[name]
	if !ok {
		lk = &sync.Mutex{}
		s.lifecycles[name] = lk
	}
	return lk
}

// named returns a deployed pipeline by name, nil when absent.
func (s *Service) named(name string) *managed {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pipelines[name]
}

// drop removes the map entry when it still is `want` (nil drops nothing), so
// a failed replacement does not leave a stopped instance behind reporting a
// live-looking status.
func (s *Service) drop(name string, want *managed) {
	if want == nil {
		return
	}
	s.mu.Lock()
	if cur, ok := s.pipelines[name]; ok && cur == want {
		delete(s.pipelines, name)
	}
	s.mu.Unlock()
}

// startManaged builds, registers and starts one instance from an already
// verified configuration. pip is the IR built by the verify composition —
// Deploy hands its single parse through, so a continuous pipeline is never
// built twice (candidate 05); Resume passes the instance's retained IR. The
// cross-process store lease is acquired BEFORE the store opens and released
// by the instance's goroutine before `done` closes: one writer per pipeline
// store, across processes (candidate 08). The caller owns the per-pipeline
// lifecycle mutex.
func (s *Service) startManaged(ctx context.Context, cfg *config.Pipeline, pip *ir.Pipeline, file string) (*managed, error) {
	lease, err := s.opts.Stores.Acquire(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("deploy: pipeline %q: %w; use the admin/MCP surface against the running process (the `trigger` tool for job runs, `dlq_replay` for dead letters) instead of starting a second engine on the same store", cfg.Name, err)
	}
	// The instance outlives the Deploy/Resume CALL: an MCP/HTTP request
	// context is canceled when its response is sent, which must not stop a
	// deployed pipeline. Values ride along; cancellation is the Service's
	// business (shutdown/Stop).
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m := &managed{name: cfg.Name, file: file, cfg: cfg, pip: pip, cancel: cancel, done: make(chan struct{}), started: s.opts.Clock(), status: stateRunning, lease: lease}
	if cfg.IsJob() {
		m.kind = "job"
		st, err := s.opts.Stores.Open(cfg.Name)
		if err != nil {
			cancel()
			m.releaseLease()
			return nil, err
		}
		opts := jobs.Options{}
		opts.EngineOptions = engine.DefaultOptions().WithLimits(cfg.Limits)
		opts.EngineOptions.SinkWrapper = s.tailWrapper(cfg)
		opts.EngineOptions.Obs = s.opts.Obs
		opts.EngineOptions.SpoolRetention = s.opts.SpoolRetention
		if cfg.Telemetry != nil {
			opts.EngineOptions.SpanSampleRate = cfg.Telemetry.SpanSampleRate
		}
		jm, err := jobs.New(cfg, file, st, s.reg, opts)
		if err != nil {
			cancel()
			m.releaseLease()
			return nil, err
		}
		m.jobs = jm
		// Start performs crash recovery + catchup synchronously; the
		// instance becomes visible only after it returned. Installing first
		// would let a concurrent Trigger admit a run that recovery resumes a
		// second time (one run id, two engines — the deploy/trigger race).
		startup := make(chan error, 1)
		go func() {
			defer close(m.done)
			defer m.releaseLease()
			err := jm.Start(runCtx)
			startup <- err
			if err != nil {
				m.setErr(err.Error())
				m.advance(stateRunning, stateFailed)
				return
			}
			<-runCtx.Done()
			jm.Stop()
		}()
		if err := <-startup; err != nil {
			return nil, fmt.Errorf("deploy: pipeline %q: start: %w", cfg.Name, err)
		}
		s.install(m)
		return m, nil
	}

	m.kind = "continuous"
	if cfg.IsBatch() {
		m.kind = "batch"
	}
	if pip == nil {
		// Defensive: Resume of an instance that never retained an IR (only
		// possible for hand-built managers) rebuilds it here.
		built, diags := verify.Build(cfg, s.reg, verify.Options{})
		if built == nil {
			cancel()
			m.releaseLease()
			return nil, fmt.Errorf("deploy: %s", diags.FirstErrorText())
		}
		pip = built
		m.pip = built
	}
	st, err := s.opts.Stores.Open(cfg.Name)
	if err != nil {
		cancel()
		m.releaseLease()
		return nil, err
	}
	opts := engine.DefaultOptions().WithLimits(cfg.Limits)
	opts.SinkWrapper = s.tailWrapper(cfg)
	opts.Obs = s.opts.Obs
	opts.SpoolRetention = s.opts.SpoolRetention
	if cfg.Telemetry != nil {
		opts.SpanSampleRate = cfg.Telemetry.SpanSampleRate
	}
	eng, err := engine.New(pip, st, s.reg, opts)
	if err != nil {
		cancel()
		m.releaseLease()
		return nil, err
	}
	m.eng = eng
	s.install(m)
	runDone := make(chan error, 1)
	go func() { runDone <- eng.Run(runCtx) }()
	// One terminal-status watcher for both engine shapes (batch and
	// continuous): a batch completes by quiescing, while a continuous
	// pipeline only stops with a live runCtx when it failed (source
	// failure, worker-fatal) — the outcome decides, never a settle poll.
	go s.watchEngineCompletion(m, eng, runDone, runCtx)
	// Wait briefly for readiness so status immediately reflects reality.
	for i := 0; i < 200 && !eng.Ready(); i++ {
		time.Sleep(2 * time.Millisecond)
	}
	return m, nil
}

// install registers the instance, replacing any previous entry under one
// s.mu critical section: Status/Drain never observe a half-installed map.
func (s *Service) install(m *managed) {
	s.mu.Lock()
	s.pipelines[m.name] = m
	s.mu.Unlock()
}

// watchEngineCompletion derives a pipeline's terminal status from the engine
// outcome once its run ends (candidate 02): failed when the engine stopped
// itself (source failure, worker-fatal), completed otherwise. The daemon
// stays up — a finished pipeline keeps its history and its admin surface. A
// run canceled by the service shutting down leaves the status to shutdown()
// (it owns stopped/drained/paused). The lease is released before done closes,
// so a replacement can re-acquire immediately.
func (s *Service) watchEngineCompletion(m *managed, eng *engine.Engine, runDone <-chan error, runCtx context.Context) {
	defer close(m.done)
	defer m.releaseLease()
	outcome := eng.Wait(runCtx, runDone, engine.WaitOptions{})
	if runCtx.Err() != nil {
		return // service shutting down: shutdown() owns the status
	}
	if outcome.Status == engine.RunFailed {
		m.setErr(outcome.FailureText())
		m.advance(stateRunning, stateFailed)
	} else {
		m.advance(stateRunning, stateCompleted)
	}
	// The status transition event carries the same snapshot shape as the
	// SSE ticker's periodic event: one event type, one payload type — the
	// UI's render(JSON.parse(e.data)) handles both (adversarial review
	// follow-up 2026-09-24: a bare pipeline name here made the UI render a
	// string as a table).
	s.emit("status", s.Status())
}

// shutdown stops one instance and reports whether it fully stopped. runCtx
// is canceled FIRST: the engine's ctx derives from it, so this alone stops
// the run — and it makes the terminal-status watcher see the shutdown as a
// caller cancellation (the watcher then leaves the status to shutdown, which
// owns paused/drained) instead of racing in a "completed" for a run that was
// stopped. For job pipelines the instance's own goroutine calls jm.Stop()
// after runCtx is done and only then closes m.done, so a true result means
// every run reached and persisted its terminal state. The 15s guard covers a
// goroutine that cannot return at all; callers treat false as "still
// running" and refuse to start a replacement.
func (m *managed) shutdown() bool {
	m.cancel()
	if m.eng != nil {
		m.eng.Close()
	}
	if m.jobs != nil {
		m.jobs.Stop()
	}
	select {
	case <-m.done:
		return true
	case <-time.After(15 * time.Second):
		return false
	}
}

// of returns a managed pipeline by name.
func (s *Service) of(name string) (*managed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("pipeline %q is not deployed", name)
	}
	return m, nil
}

// Stop shuts everything down (process exit).
func (s *Service) Stop() {
	s.stopped.Store(true)
	s.mu.Lock()
	ms := make([]*managed, 0, len(s.pipelines))
	for _, m := range s.pipelines {
		ms = append(ms, m)
	}
	s.mu.Unlock()
	for _, m := range ms {
		m.shutdown()
	}
}

// Ready reports whether the service is still serving: false once Stop has
// begun, so the /ready endpoint drains the pod before the process exits.
// Pipeline-level failures are visible in the status snapshot, not here — a
// failed pipeline is not a reason to restart the process.
func (s *Service) Ready() bool { return !s.stopped.Load() }

// Status snapshots every deployed pipeline (rates are deltas over the last
// snapshot window).
type PipelineStatus struct {
	Pipeline     string         `json:"pipeline"`
	Mode         string         `json:"mode"`
	Status       string         `json:"status"`
	Error        string         `json:"error,omitempty"`
	Nodes        []NodeStatus   `json:"nodes"`
	InFlight     int            `json:"in_flight"`
	Checkpoint   int64          `json:"checkpoint"`
	MessagesIn   int64          `json:"messages_in"`
	Committed    int64          `json:"committed"`
	DeadLettered int64          `json:"dead_lettered"`
	MsgPerSec    float64        `json:"messages_per_sec"`
	RecentRuns   []store.JobRun `json:"recent_runs,omitempty"`
}

type NodeStatus struct {
	Node    string `json:"node"`
	Section string `json:"section"`
	Plugin  string `json:"plugin,omitempty"`
}

func (s *Service) Status() []PipelineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.opts.Clock()
	s.rateMu.Lock()
	window := now.Sub(s.rateAt).Seconds()
	if window <= 0 {
		window = 1
	}
	s.rateMu.Unlock()

	out := make([]PipelineStatus, 0, len(s.pipelines))
	for _, m := range s.pipelines {
		m.mu.Lock()
		st := PipelineStatus{Pipeline: m.name, Mode: m.kind, Status: m.status, Error: m.err}
		m.mu.Unlock()
		for _, name := range m.cfg.Order {
			var n *config.Node
			switch {
			case m.cfg.Sources[name] != nil:
				n = m.cfg.Sources[name]
			case m.cfg.Transforms[name] != nil:
				n = m.cfg.Transforms[name]
			default:
				n = m.cfg.Sinks[name]
			}
			if n != nil {
				st.Nodes = append(st.Nodes, NodeStatus{Node: name, Section: string(n.Section), Plugin: n.Plugin})
			}
		}
		if m.eng != nil {
			outstanding, committedThrough, _ := m.eng.CommitSnapshot()
			st.InFlight = outstanding
			st.Checkpoint = committedThrough
			st.MessagesIn = m.eng.Metrics.MessagesIn.Load()
			st.Committed = m.eng.Metrics.CommittedCount.Load()
			st.DeadLettered = m.eng.Metrics.DeadLettered.Load()
			s.recordSourceCounters(m.name, m.eng)
		}
		if m.jobs != nil {
			if runs, err := s.jobsRuns(m.name, 5); err == nil {
				st.RecentRuns = runs
				// Newest first: the FIRST runnable run is the latest one.
				// The old loop kept overwriting, so several runnable runs
				// reported the OLDEST status (candidate 08 — one status
				// derivation).
				for _, r := range runs {
					if store.IsRunnableStatus(r.Status) {
						st.Status = "run:" + r.Status
						break
					}
				}
			}
		}
		s.rateMu.Lock()
		prev := s.lastSnap[m.name]
		st.MsgPerSec = float64(st.MessagesIn-prev) / window
		s.lastSnap[m.name] = st.MessagesIn
		s.rateMu.Unlock()
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pipeline < out[j].Pipeline })
	s.rateMu.Lock()
	s.rateAt = now
	s.rateMu.Unlock()
	// Push the pipeline-level gauges on every snapshot (the SSE stream and
	// UI poll Status; /metrics scrapes the values recorded here).
	for _, st := range out {
		m := s.pipelines[st.Pipeline]
		spoolDepth := 0
		if m != nil && m.eng != nil {
			_, committedThrough, arrivedMax := m.eng.CommitSnapshot()
			spoolDepth = int(arrivedMax - committedThrough)
			if spoolDepth < 0 {
				spoolDepth = 0
			}
		}
		s.opts.Obs.SetGauges(st.Pipeline, st.InFlight, spoolDepth, st.Status == "paused")
	}
	return out
}

// recordSourceCounters writes the DELTA of every source health counter since
// the previous Status poll to telemetry (log-collection design §2.6.3): the
// source exposes monotonic totals, the instrument wants increments. A value
// below its baseline means the counter reset (source restart or pipeline
// redeploy) — the baseline is moved and no negative delta is written. The
// engine/registry stay telemetry-free; this is the one polling seam.
func (s *Service) recordSourceCounters(pipeline string, eng *engine.Engine) {
	counters := eng.SourceCounters()
	s.counterMu.Lock()
	defer s.counterMu.Unlock()
	if s.lastCounters == nil {
		s.lastCounters = map[string]int64{}
	}
	for node, nodeCounters := range counters {
		for counter, v := range nodeCounters {
			key := pipeline + "\x00" + node + "\x00" + counter
			prev, seen := s.lastCounters[key]
			s.lastCounters[key] = v
			if !seen || v < prev {
				continue // first observation / reset: baseline only
			}
			if delta := v - prev; delta > 0 {
				s.opts.Obs.RecordSourceCounter(pipeline, node, counter, delta)
			}
		}
	}
}

func (s *Service) jobsRuns(name string, limit int) ([]store.JobRun, error) {
	st, err := s.opts.Stores.Open(name)
	if err != nil {
		return nil, err
	}
	return st.JobRuns(name, limit)
}

// Jobs lists run history for a job pipeline.
func (s *Service) Jobs(pipeline string, limit int) ([]store.JobRun, error) {
	if _, err := s.of(pipeline); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	return s.jobsRuns(pipeline, limit)
}

// Trigger fires a job run. wait=true blocks for the terminal record
// (backfills); wait=false returns the created pending record immediately —
// its RunID is the handle for jobs polling, so async triggers are never
// null on the admin/MCP surfaces (candidate 08).
func (s *Service) Trigger(ctx context.Context, pipeline string, parameters map[string]any, wait bool) (*store.JobRun, error) {
	m, err := s.of(pipeline)
	if err != nil {
		return nil, err
	}
	if m.jobs == nil {
		return nil, fmt.Errorf("pipeline %q is not a job pipeline", pipeline)
	}
	_, jr, err := m.jobs.Trigger(ctx, parameters, wait)
	if err != nil {
		return nil, err
	}
	s.emit("job", jr)
	return jr, nil
}

// Tail samples the most recent deliveries for one node (bounded ring).
func (s *Service) Tail(node string, n int) []TailEntry {
	if n <= 0 {
		n = 20
	}
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	entries := s.tails[node]
	if len(entries) > n {
		entries = entries[len(entries)-n:]
	}
	return append([]TailEntry(nil), entries...)
}

func (s *Service) tailWrapper(cfg *config.Pipeline) func(node string, snk registry.Sink) registry.Sink {
	// Tail entries show the payload document; patterns are compiled against
	// the payload root (meta.* patterns have nothing to match there).
	redact := compileRedactForRoot(nil, "payload")
	if cfg.Telemetry != nil {
		redact = compileRedactForRoot(cfg.Telemetry.Redact, "payload")
	}
	return func(node string, snk registry.Sink) registry.Sink {
		return &tailSink{inner: snk, svc: s, node: node, redact: redact}
	}
}

type tailSink struct {
	inner  registry.Sink
	svc    *Service
	node   string
	redact []redactor
}

func (t *tailSink) Write(ctx context.Context, msgs []registry.Message) error {
	err := t.inner.Write(ctx, msgs)
	if err == nil {
		t.svc.recordTail(t.node, msgs, t.redact)
	}
	return err
}

func (t *tailSink) Close() error { return t.inner.Close() }

func (s *Service) recordTail(node string, msgs []registry.Message, redact []redactor) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	for _, m := range msgs {
		payload := string(m.Out)
		if payload == "" {
			payload = string(m.Raw)
		}
		// Redaction is presentation-only (tail entries); the spool, dead
		// letters and deliveries are the data path and are never altered.
		payload = redactJSON(payload, redact)
		if len(payload) > 512 {
			payload = payload[:512] + "…"
		}
		isReplay, _ := m.Meta["is_replay"].(bool)
		s.tails[node] = append(s.tails[node], TailEntry{
			Node: node, MessageID: m.ID, Payload: payload, At: time.Now().UTC(), IsReplay: isReplay,
		})
		if excess := len(s.tails[node]) - 100; excess > 0 {
			s.tails[node] = s.tails[node][excess:]
		}
	}
}

// DeadLetterQuery filters dead letters; where is a CEL predicate over
// {payload, meta}, compiled with the pipeline's constants (the same surface
// the CLI replay uses).
func (s *Service) DeadLetterQuery(pipeline, since, where string, limit int) ([]store.DeadLetter, error) {
	m, err := s.of(pipeline)
	if err != nil {
		return nil, err
	}
	st, err := s.opts.Stores.Open(pipeline)
	if err != nil {
		return nil, err
	}
	sinceT, err := dlq.Since(since, time.Now())
	if err != nil {
		return nil, err
	}
	dls, err := st.DeadLettersSince(pipeline, sinceT)
	if err != nil {
		return nil, err
	}
	kept, err := dlq.FilterDeadLetters(dls, dlq.Filter{Where: where, Limit: limit}, m.cfg.Constants)
	if err != nil {
		return nil, err
	}
	return redactDeadLetters(m.cfg, kept), nil
}

// redactDeadLetters masks telemetry.redact-matched values in dead letters
// about to cross an ops surface (admin REST + MCP tools): the SAME compiled
// patterns the tail wrapper uses, applied to both roots — the payload
// patterns against the raw document, the meta.* patterns against the meta
// map. Presentation-only, like the tail: the stored rows stay raw so
// DeadLetterReplay re-injects the original bytes (the data path is never
// altered).
func redactDeadLetters(cfg *config.Pipeline, dls []store.DeadLetter) []store.DeadLetter {
	var payloadRedact, metaRedact []redactor
	if cfg != nil && cfg.Telemetry != nil {
		payloadRedact = compileRedactForRoot(cfg.Telemetry.Redact, "payload")
		metaRedact = compileRedactForRoot(cfg.Telemetry.Redact, "meta")
	}
	for i := range dls {
		dls[i].Raw = []byte(redactJSON(string(dls[i].Raw), payloadRedact))
		if len(metaRedact) > 0 {
			if masked, ok := redactValue(dls[i].Meta, metaRedact).(map[string]any); ok {
				dls[i].Meta = masked
			}
		}
	}
	return dls
}

// DeadLetterReplay re-injects selected dead letters into the RUNNING
// pipeline's engine at a target node (default: each letter's origin node).
// Selection is the shared dlq contract; the injection message carries the
// dead letter's codec (candidate 01).
func (s *Service) DeadLetterReplay(pipeline string, ids []int64, at string) (int, error) {
	m, err := s.of(pipeline)
	if err != nil {
		return 0, err
	}
	if m.eng == nil {
		// Job pipelines: re-run their dead letters as a fresh manual run.
		return 0, fmt.Errorf("pipeline %q runs in job mode; replay its runs via a manual trigger or `eventboat replay --job`", pipeline)
	}
	st, err := s.opts.Stores.Open(pipeline)
	if err != nil {
		return 0, err
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil {
		return 0, err
	}
	reqs, err := dlq.Select(dls, dlq.Filter{IDs: ids, At: at}, m.cfg.Constants)
	if err != nil {
		return 0, err
	}
	replayed := 0
	for _, req := range reqs {
		if _, err := m.eng.InjectReplay(req.Node, req.Message()); err != nil {
			return replayed, err
		}
		replayed++
	}
	if replayed > 0 {
		waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = m.eng.WaitCommit(waitCtx)
		cancel()
	}
	return replayed, nil
}

// Drain stops a pipeline's sources and waits for in-flight work to commit —
// and, for job pipelines, for every run to persist its terminal state
// (candidate 08: Stop waits). The pipeline stays deployed (drained); Resume
// restarts it.
func (s *Service) Drain(pipeline string) error {
	lk := s.lifecycle(pipeline)
	lk.Lock()
	defer lk.Unlock()
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	switch m.state() {
	case stateDrained:
		return nil // idempotent
	case stateRunning:
	default:
		return fmt.Errorf("pipeline %q is %s; only a running pipeline can be drained", pipeline, m.state())
	}
	if !m.shutdown() {
		return fmt.Errorf("drain: pipeline %q did not stop within the drain bound; runs may still be executing", pipeline)
	}
	if err := m.transition(stateDrained); err != nil {
		return err
	}
	s.emit("status", s.Status())
	return nil
}

// Pause stops source pulls; Resume restarts from persisted source states
// (at-least-once covers the pause window).
func (s *Service) Pause(pipeline string) error {
	lk := s.lifecycle(pipeline)
	lk.Lock()
	defer lk.Unlock()
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	switch m.state() {
	case statePaused:
		return nil // idempotent
	case stateRunning:
	default:
		return fmt.Errorf("pipeline %q is %s; only a running pipeline can be paused", pipeline, m.state())
	}
	if !m.shutdown() {
		return fmt.Errorf("pause: pipeline %q did not stop within the drain bound; runs may still be executing", pipeline)
	}
	if err := m.transition(statePaused); err != nil {
		return err
	}
	s.emit("status", s.Status())
	return nil
}

// Resume restarts a paused or drained pipeline (candidate 08: the old
// Resume-after-Drain was a silent no-op reporting `running` while nothing
// ran). The previous instance was fully stopped by Pause/Drain — including
// its runs' terminal persistence and the release of its store lease — so the
// replacement starts cleanly. Terminal instances (completed/failed) are
// replaced by Deploy, never resumed in place.
func (s *Service) Resume(ctx context.Context, pipeline string) error {
	lk := s.lifecycle(pipeline)
	lk.Lock()
	defer lk.Unlock()
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	switch m.state() {
	case stateRunning:
		return nil // idempotent
	case statePaused, stateDrained:
		// Restart below.
	default:
		return fmt.Errorf("pipeline %q is %s (terminal); deploy it again to restart", pipeline, m.state())
	}
	if _, err := s.startManaged(ctx, m.cfg, m.pip, m.file); err != nil {
		return err
	}
	s.emit("status", s.Status())
	return nil
}
