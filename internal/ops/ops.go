// Package ops is the operations service: the single implementation behind
// the MCP tools and the Admin REST endpoints (redesign-v3.md §3.4). Every
// write path goes through verify-first; there is no bypass channel.
package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/explain"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/jobs"
	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testrun"
)

// Options configures the service.
type Options struct {
	DataDir string // deployed pipeline files live under <DataDir>/pipelines
	Reg     *registry.Registry
	// StoreFor returns the durable store for one pipeline (shared SQLite by
	// default; tests may inject per-pipeline memory stores).
	StoreFor func(pipeline string) (store.Store, error)
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

	mu        sync.Mutex
	pipelines map[string]*managed

	tailMu sync.Mutex
	tails  map[string][]TailEntry // node → recent deliveries (bounded)

	rateMu   sync.Mutex
	lastSnap map[string]int64 // pipeline → messages_in at last snapshot (rate deltas)
	rateAt   time.Time

	subMu sync.Mutex
	subs  map[chan Event]struct{}
}

// managed is one deployed pipeline: either a continuous engine or a job
// manager (per its run.mode).
type managed struct {
	name    string
	file    string
	cfg     *config.Pipeline
	kind    string // "continuous" | "job"
	eng     *engine.Engine
	jobs    *jobs.Manager
	cancel  context.CancelFunc
	done    chan struct{}
	paused  bool
	status  string
	err     string
	started time.Time
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
	if opts.StoreFor == nil {
		opts.StoreFor = func(pipeline string) (store.Store, error) {
			dir := filepath.Join(opts.DataDir, "stores")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			return store.OpenSQLite(filepath.Join(dir, "pipeline.db"))
		}
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Service{
		opts:      opts,
		reg:       opts.Reg,
		pipelines: map[string]*managed{},
		tails:     map[string][]TailEntry{},
		subs:      map[chan Event]struct{}{},
		lastSnap:  map[string]int64{},
		rateAt:    time.Now(),
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
func (s *Service) Verify(configContent string) []config.Diagnostic {
	lr := config.LoadBytes("submitted.yaml", []byte(configContent))
	diags := append([]config.Diagnostic(nil), lr.Diagnostics...)
	if lr.Pipeline != nil {
		_, buildDiags := ir.Build(lr.Pipeline, s.reg, starhost.DefaultOptions(), nil)
		diags = append(diags, buildDiags...)
	}
	return diags
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

// Explain renders the deterministic walkthrough of a configuration.
func (s *Service) Explain(configContent, message string, topology bool) (string, error) {
	lr := config.LoadBytes("submitted.yaml", []byte(configContent))
	if lr.HasErrors() {
		return "", fmt.Errorf("explain: config errors: %s", firstErrText(lr.Diagnostics))
	}
	pip, diags := ir.Build(lr.Pipeline, s.reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		return "", fmt.Errorf("explain: %s", firstErrText(diags))
	}
	if topology {
		return explain.TopologyMermaid(pip) + "\n\n" + explain.TopologyASCII(pip), nil
	}
	opts := explain.Options{}
	if message != "" {
		opts.Message = []byte(message)
	}
	return explain.Trace(pip, opts)
}

// Deploy verifies then swaps one pipeline: drain the old instance, start the
// new one (§3.4 iron rule: no write path bypasses verification).
func (s *Service) Deploy(ctx context.Context, configContent string) (map[string]any, error) {
	if diags := s.Verify(configContent); hasErr(diags) {
		return nil, fmt.Errorf("deploy rejected: verify failed:\n%s", diagLines(diags))
	}
	lr := config.LoadBytes("submitted.yaml", []byte(configContent))
	cfg := lr.Pipeline

	// Persist the deployed config (jobs reload it per run; restarts re-read it).
	if err := os.MkdirAll(filepath.Join(s.opts.DataDir, "pipelines"), 0o755); err != nil {
		return nil, err
	}
	file := filepath.Join(s.opts.DataDir, "pipelines", cfg.Name+".yaml")
	if err := os.WriteFile(file, []byte(configContent), 0o644); err != nil {
		return nil, err
	}

	// Stop the previous instance (drain) before starting the replacement.
	previous := "none"
	s.mu.Lock()
	if old, ok := s.pipelines[cfg.Name]; ok {
		previous = old.kind
		s.mu.Unlock()
		old.shutdown()
		s.mu.Lock()
		delete(s.pipelines, cfg.Name)
	}
	m, err := s.startManaged(ctx, cfg, file)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.emit("deploy", map[string]any{"pipeline": cfg.Name, "mode": m.kind, "replaced": previous})
	return map[string]any{
		"pipeline": cfg.Name, "mode": m.kind, "replaced": previous,
		"nodes": len(cfg.Order),
	}, nil
}

func (s *Service) startManaged(ctx context.Context, cfg *config.Pipeline, file string) (*managed, error) {
	runCtx, cancel := context.WithCancel(ctx)
	m := &managed{name: cfg.Name, file: file, cfg: cfg, cancel: cancel, done: make(chan struct{}), started: s.opts.Clock()}
	if cfg.IsJob() {
		m.kind = "job"
		st, err := s.opts.StoreFor(cfg.Name)
		if err != nil {
			cancel()
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
			return nil, err
		}
		m.jobs = jm
		s.pipelines[cfg.Name] = m
		go func() {
			defer close(m.done)
			if err := jm.Start(runCtx); err != nil {
				m.err = err.Error()
			}
		}()
	} else {
		m.kind = "continuous"
		pip, diags := ir.Build(cfg, s.reg, starhost.DefaultOptions(), nil)
		if pip == nil {
			cancel()
			return nil, fmt.Errorf("deploy: %s", firstErrText(diags))
		}
		st, err := s.opts.StoreFor(cfg.Name)
		if err != nil {
			cancel()
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
			return nil, err
		}
		m.eng = eng
		s.pipelines[cfg.Name] = m
		go func() {
			defer close(m.done)
			if err := eng.Run(runCtx); err != nil {
				m.err = err.Error()
			}
		}()
		// Wait briefly for readiness so status immediately reflects reality.
		for i := 0; i < 200 && !eng.Ready(); i++ {
			time.Sleep(2 * time.Millisecond)
		}
	}
	m.status = "running"
	return m, nil
}

func (m *managed) shutdown() {
	if m.eng != nil {
		m.eng.Close()
	}
	if m.jobs != nil {
		m.jobs.Stop()
	}
	m.cancel()
	select {
	case <-m.done:
	case <-time.After(15 * time.Second):
	}
	m.status = "stopped"
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
		st := PipelineStatus{Pipeline: m.name, Mode: m.kind, Status: m.status, Error: m.err}
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
		}
		if m.jobs != nil {
			if runs, err := s.jobsRuns(m.name, 5); err == nil {
				st.RecentRuns = runs
				for _, r := range runs {
					if r.Runnable() {
						st.Status = "run:" + r.Status
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

func (s *Service) jobsRuns(name string, limit int) ([]store.JobRun, error) {
	st, err := s.opts.StoreFor(name)
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

// Trigger fires a job run (optionally waiting for it — backfills).
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
// {payload, meta}.
func (s *Service) DeadLetterQuery(pipeline, since, where string, limit int) ([]store.DeadLetter, error) {
	m, err := s.of(pipeline)
	if err != nil {
		return nil, err
	}
	st, err := s.opts.StoreFor(pipeline)
	if err != nil {
		return nil, err
	}
	sinceT := time.Time{}
	if since != "" {
		d, err := config.ParseDuration(since)
		if err != nil {
			return nil, fmt.Errorf("--since %q: %w", since, err)
		}
		sinceT = time.Now().Add(-d)
	}
	dls, err := st.DeadLettersSince(pipeline, sinceT)
	if err != nil {
		return nil, err
	}
	if where != "" {
		env, err := celhost.NewEnv(nil, nil)
		if err != nil {
			return nil, err
		}
		pred, err := env.Compile(where)
		if err != nil {
			return nil, fmt.Errorf("--where: %w", err)
		}
		var kept []store.DeadLetter
		for _, dl := range dls {
			var payload any
			_ = json.Unmarshal(dl.Raw, &payload)
			if ok, evalErr := pred.Eval(payload, dl.Meta); evalErr == nil && ok {
				kept = append(kept, dl)
			}
		}
		dls = kept
	}
	if limit > 0 && len(dls) > limit {
		dls = dls[:limit]
	}
	return redactDeadLetters(m.cfg, dls), nil
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
func (s *Service) DeadLetterReplay(pipeline string, ids []int64, at string) (int, error) {
	m, err := s.of(pipeline)
	if err != nil {
		return 0, err
	}
	if m.eng == nil {
		// Job pipelines: re-run their dead letters as a fresh manual run.
		return 0, fmt.Errorf("pipeline %q runs in job mode; replay its runs via a manual trigger or `eventboat replay --job`", pipeline)
	}
	st, err := s.opts.StoreFor(pipeline)
	if err != nil {
		return 0, err
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil {
		return 0, err
	}
	replayed := 0
	for _, dl := range dls {
		if len(ids) > 0 && !containsInt(ids, dl.ID) {
			continue
		}
		node := dl.Node
		if at != "" {
			node = at
		}
		if _, err := m.eng.InjectReplay(node, dl.Raw, dl.Meta, dl.MessageID); err != nil {
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

// Drain stops a pipeline's sources and waits for in-flight work to commit.
// The pipeline stays deployed (drained).
func (s *Service) Drain(pipeline string) error {
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	m.shutdown()
	m.status = "drained"
	s.emit("status", pipeline)
	return nil
}

// Pause stops source pulls; Resume restarts from persisted source states
// (at-least-once covers the pause window).
func (s *Service) Pause(pipeline string) error {
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	if m.paused {
		return nil
	}
	m.shutdown()
	m.paused = true
	m.status = "paused"
	s.emit("status", pipeline)
	return nil
}

func (s *Service) Resume(ctx context.Context, pipeline string) error {
	m, err := s.of(pipeline)
	if err != nil {
		return err
	}
	if !m.paused {
		return nil
	}
	cfg, file := m.cfg, m.file
	s.mu.Lock()
	delete(s.pipelines, pipeline)
	nm, err := s.startManaged(ctx, cfg, file)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	_ = nm
	s.emit("status", pipeline)
	return nil
}

func containsInt(list []int64, v int64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func hasErr(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

func firstErrText(diags []config.Diagnostic) string {
	for _, d := range diags {
		if d.Severity == "error" {
			return d.Error()
		}
	}
	if len(diags) > 0 {
		return diags[0].Error()
	}
	return "unknown"
}

func diagLines(diags []config.Diagnostic) string {
	var b strings.Builder
	for _, d := range diags {
		if d.Severity == "error" {
			fmt.Fprintf(&b, "  %s\n", d.Error())
		}
	}
	return b.String()
}
