// Package jobs implements the job-pipeline runtime (redesign-v3.md §5.8):
// cron scheduling, catchup_window compensation, overlap admission,
// skip_if_successful, per-run engine lifecycle (pending → running →
// committing → success|partial|failed|canceled), typed parameters with the
// cursor/now engine bindings, failure/success hooks and run-history
// retention. Scheduling lives here — never in source plugins.
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Options tunes the jobs manager.
type Options struct {
	Clock         func() time.Time
	NewRunID      func() string
	EngineOptions engine.Options // base; limits are applied per pipeline
	// CatchupTicksSkipped / OverlapSkips count admission decisions (surfaced
	// as OTel counters in M2 step 5).
	CatchupTicksSkipped func(delta int64)
	OverlapSkips        func(delta int64)
}

// obs returns the telemetry sink from the engine options (nil-safe).
func (o Options) obs() *obs.Obs { return o.EngineOptions.Obs }

func (o Options) clock() func() time.Time {
	if o.Clock != nil {
		return o.Clock
	}
	return time.Now
}

func (o Options) runID() string {
	if o.NewRunID != nil {
		return o.NewRunID()
	}
	return uuid.New().String()
}

// Manager runs one job pipeline: scheduling, admission and run lifecycle.
type Manager struct {
	cfg  *config.Pipeline // verified configuration (metadata, run, parameters, hooks, limits)
	file string           // pipeline file (re-loaded per run for substitution)
	reg  *registry.Registry
	st   store.Store
	opts Options

	mu      sync.Mutex
	current map[string]*runningRun // active runs (overlap: all allows several)
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// admission is the pipeline-aggregated spool admission pool shared by
	// every concurrent run's engine (M2 review R17: max_in_flight aggregates
	// across overlap:all runs instead of multiplying per run). Lazily sized
	// from the first run's resolved limits — same pipeline config each run.
	admission chan struct{}
}

type runningRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	jr     store.JobRun
}

// New builds a Manager for one verified job pipeline.
func New(cfg *config.Pipeline, file string, st store.Store, reg *registry.Registry, opts Options) (*Manager, error) {
	if !cfg.IsJob() {
		return nil, fmt.Errorf("jobs: pipeline %q is not a job pipeline (run.mode: job required)", cfg.Name)
	}
	if opts.EngineOptions.Clock == nil {
		opts.EngineOptions.Clock = opts.clock()
	}
	if opts.EngineOptions.NewID == nil {
		opts.EngineOptions.NewID = func() string { return uuid.New().String() }
	}
	count := opts.CatchupTicksSkipped
	if count == nil {
		count = func(int64) {}
	}
	opts.CatchupTicksSkipped = count
	skips := opts.OverlapSkips
	if skips == nil {
		skips = func(int64) {}
	}
	opts.OverlapSkips = skips
	return &Manager{
		cfg: cfg, file: file, reg: reg, st: st, opts: opts,
		current: map[string]*runningRun{},
		stopCh:  make(chan struct{}),
	}, nil
}

// Start resumes interrupted runs, performs catchup, and (when scheduled)
// fires ticks until ctx is done.
func (m *Manager) Start(ctx context.Context) error {
	// Crash recovery: runs in pending/running/committing resume from the
	// persisted watermark + spool replay (invariants 3 & 7).
	runnable, err := m.st.RunnableJobRuns(m.cfg.Name)
	if err != nil {
		return fmt.Errorf("jobs: recovery: %w", err)
	}
	for _, jr := range runnable {
		m.spawnResume(ctx, jr)
	}

	if m.cfg.Run.Schedule != "" {
		m.maybeCatchup(ctx)
		m.wg.Add(1)
		go m.scheduleLoop(ctx)
	}
	return nil
}

// Stop cancels active runs (bounded drain + terminal dead letters, review
// R2) and waits for the manager's goroutines.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	close(m.stopCh)
	current := make([]*runningRun, 0, len(m.current))
	for _, rr := range m.current {
		current = append(current, rr)
	}
	m.mu.Unlock()
	for _, rr := range current {
		rr.cancel()
	}
	m.wg.Wait()
}

// ActiveRuns lists the run-ids currently executing.
func (m *Manager) ActiveRuns() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.current))
	for id := range m.current {
		out = append(out, id)
	}
	return out
}

// scheduleLoop fires scheduled ticks; each tick's identity is its scheduled
// time (skip_if_successful and catchup idempotence key on it).
func (m *Manager) scheduleLoop(ctx context.Context) {
	defer m.wg.Done()
	sched, err := cron.ParseStandard(m.cfg.Run.Schedule)
	if err != nil {
		return // verified earlier; unreachable in practice
	}
	for {
		now := m.opts.clock()()
		next := sched.Next(now)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-m.stopCh:
			timer.Stop()
			return
		case <-timer.C:
			timer.Stop()
			m.fireTick(ctx, next, "schedule")
		}
	}
}

// maybeCatchup runs at most ONE missed tick — the most recent one inside
// the catchup window (open question #9 ruling: window内补跑一次，窗外跳过计数).
func (m *Manager) maybeCatchup(ctx context.Context) {
	window := m.cfg.Run.CatchupWindow
	if window <= 0 {
		return
	}
	sched, err := cron.ParseStandard(m.cfg.Run.Schedule)
	if err != nil {
		return
	}
	lastFor, err := m.st.LastScheduledFor(m.cfg.Name)
	if err != nil || lastFor == "" {
		return // no history: nothing was missed
	}
	lastTick, err := time.Parse(time.RFC3339, lastFor)
	if err != nil {
		return
	}
	now := m.opts.clock()()
	var missed []time.Time
	for t := sched.Next(lastTick); !t.After(now); t = sched.Next(t) {
		missed = append(missed, t)
	}
	if len(missed) == 0 {
		return
	}
	var catchable *time.Time
	for i := range missed {
		if now.Sub(missed[i]) <= window {
			catchable = &missed[i]
		} else {
			m.opts.CatchupTicksSkipped(1)
			m.opts.obs().RecordCatchupSkip(m.cfg.Name)
		}
	}
	if catchable != nil {
		m.fireTick(ctx, *catchable, "catchup")
	}
}

// fireTick applies skip_if_successful and admission, then spawns the run.
func (m *Manager) fireTick(ctx context.Context, tick time.Time, trigger string) {
	tickID := tick.UTC().Format(time.RFC3339)
	if m.cfg.Run.SkipIfSuccessful {
		if ok, err := m.st.HasSuccessfulRunFor(m.cfg.Name, tickID); err == nil && ok {
			return // this tick already succeeded (also makes catchup idempotent)
		}
	}
	_, _, _ = m.spawn(ctx, nil, trigger, tickID)
}

// Trigger starts a manual run with caller-provided parameters (backfill).
// It returns the run-id; wait=true blocks until the run reaches a terminal
// state and additionally returns the final record.
func (m *Manager) Trigger(ctx context.Context, params map[string]any, wait bool) (string, *store.JobRun, error) {
	runID, _, err := m.spawn(ctx, params, "manual", "")
	if err != nil {
		return "", nil, err
	}
	if !wait {
		return runID, nil, nil
	}
	rr := m.currentOf(runID)
	if rr == nil {
		jr, err := m.st.GetJobRun(m.cfg.Name, runID)
		return runID, jr, err
	}
	select {
	case <-rr.done:
	case <-ctx.Done():
		return runID, nil, ctx.Err()
	}
	jr, err := m.st.GetJobRun(m.cfg.Name, runID)
	return runID, jr, err
}

func (m *Manager) currentOf(runID string) *runningRun {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current[runID]
}

// spawn validates parameters, applies overlap admission and starts runOnce.
func (m *Manager) spawn(ctx context.Context, params map[string]any, trigger, scheduledFor string) (string, *store.JobRun, error) {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return "", nil, fmt.Errorf("jobs: manager stopped")
	}
	// Overlap admission.
	overlap := m.cfg.Run.Overlap
	if overlap != "all" && len(m.current) > 0 {
		if overlap == "skip" {
			m.opts.OverlapSkips(1)
			m.opts.obs().RecordOverlapSkip(m.cfg.Name)
			m.mu.Unlock()
			return "", nil, fmt.Errorf("jobs: previous run still active (overlap: skip)")
		}
		// latest: cancel active runs, then proceed (review R2 semantics).
		for _, rr := range m.current {
			rr.cancel()
		}
	}
	m.mu.Unlock()

	resolved, err := m.resolveParameters(params, nil)
	if err != nil {
		return "", nil, err
	}
	runID := m.opts.runID()
	jr := store.JobRun{
		RunID:        runID,
		Pipeline:     m.cfg.Name,
		Status:       store.JobPending,
		TriggerType:  trigger,
		Parameters:   auditParams(params),
		ScheduledFor: scheduledFor,
	}
	if err := m.st.CreateJobRun(jr); err != nil {
		return "", nil, err
	}
	rr := m.runAsync(ctx, jr, params, resolved)
	_ = rr
	return runID, &jr, nil
}

func (m *Manager) spawnResume(ctx context.Context, jr store.JobRun) {
	// Resume with the trigger-time inputs (empty for scheduled defaults);
	// cursor/now re-resolve against the CURRENT watermark so the source
	// continues after the committed frontier instead of re-pulling it.
	params := map[string]any{}
	for k, v := range jr.Parameters {
		params[k] = v
	}
	resolved, err := m.resolveParameters(params, nil)
	if err == nil {
		m.runAsync(ctx, jr, params, resolved)
	} else {
		jr.Status = store.JobFailed
		jr.Error = "resume: " + err.Error()
		now := m.opts.clock()()
		jr.EndedAt = now
		_ = m.st.UpdateJobRun(jr)
	}
}

func (m *Manager) runAsync(ctx context.Context, jr store.JobRun, triggerParams, resolved map[string]any) *runningRun {
	runCtx, cancel := context.WithCancel(ctx)
	rr := &runningRun{cancel: cancel, done: make(chan struct{}), jr: jr}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		cancel()
		close(rr.done)
		return rr
	}
	m.current[jr.RunID] = rr
	m.mu.Unlock()
	go func() {
		defer close(rr.done)
		m.runOnce(runCtx, &jr, triggerParams, resolved)
		m.mu.Lock()
		delete(m.current, jr.RunID)
		m.mu.Unlock()
		cancel()
	}()
	return rr
}

// runOnce drives one full run lifecycle: load config → substitute params →
// build IR → engine to quiescence → terminal state → hooks → retention.
// The whole run is one trace span carrying trigger, parameters and terminal
// status as attributes (§6.6 spans; review R16: batch granularity).
func (m *Manager) runOnce(ctx context.Context, jr *store.JobRun, triggerParams, resolved map[string]any) {
	now := m.opts.clock()()
	jr.Status = store.JobRunning
	jr.StartedAt = now
	_ = m.st.UpdateJobRun(*jr)
	m.opts.obs().RecordJobStart(m.cfg.Name, jr.TriggerType)
	runCtx, span := m.opts.obs().Tracer().Start(ctx, "eventboat.job.run",
		trace.WithAttributes(
			attribute.String("eventboat.pipeline", m.cfg.Name),
			attribute.String("eventboat.run_id", jr.RunID),
			attribute.String("eventboat.trigger", jr.TriggerType),
		))
	defer span.End()
	ctx = runCtx

	fail := func(status, msg string) {
		jr.Status = status
		jr.Error = msg
		jr.EndedAt = m.opts.clock()()
		_ = m.st.UpdateJobRun(*jr)
		span.SetAttributes(attribute.String("eventboat.status", status))
		if msg != "" {
			span.RecordError(fmt.Errorf("%s", msg))
		}
		m.opts.obs().RecordJobEnd(m.cfg.Name, status, jr.EndedAt.Sub(jr.StartedAt),
			jr.RowsRead, jr.Delivered, jr.DeadLettered)
		if status == store.JobFailed || status == store.JobPartial {
			m.fireHook(ctx, "failure", jr)
		}
		if status == store.JobSuccess {
			m.fireHook(ctx, "success", jr)
		}
		m.applyRetention()
	}

	// Fresh config per run (trigger-time parameter values are substituted
	// into a clean tree; nothing leaks between runs).
	lr := config.LoadFile(m.file)
	if lr.HasErrors() {
		fail(store.JobFailed, "run: reload config: "+lr.Diagnostics[0].Error())
		return
	}
	cfg := lr.Pipeline

	// Per-source cursor resolution: `cursor` binds to each pull source's own
	// persisted watermark (review R9).
	sourceWatermarks := m.sourceWatermarks(cfg)
	valuesFor := func(sourceNode string) map[string]any {
		per := map[string]any{}
		for name, val := range resolved {
			if s, ok := val.(string); ok && s == "cursor" {
				if wm, ok := sourceWatermarks[sourceNode]; ok {
					per[name] = wm
				} else {
					per[name] = ""
				}
				continue
			}
			per[name] = val
		}
		return per
	}
	if diags := config.SubstituteParameters(cfg, globalValues(resolved, sourceWatermarks), valuesFor); len(diags) > 0 {
		fail(store.JobFailed, "run: parameter substitution: "+diags[0].Error())
		return
	}

	// Bindings for scripts/predicates: cursor → first pull source watermark.
	bindValues := map[string]any{}
	firstWatermark := ""
	for _, name := range cfg.Order {
		if cfg.Sources[name] != nil {
			if wm, ok := sourceWatermarks[name]; ok && firstWatermark == "" {
				firstWatermark = wm
			}
		}
	}
	for k, v := range resolved {
		if s, ok := v.(string); ok && s == "cursor" {
			bindValues[k] = firstWatermark
			continue
		}
		if s, ok := v.(string); ok && s == "now" {
			bindValues[k] = now.UTC().Format(time.RFC3339)
			continue
		}
		bindValues[k] = v
	}

	pip, diags := ir.Build(cfg, m.reg, starhost.DefaultOptions(), bindValues)
	if pip == nil {
		fail(store.JobFailed, "run: verify: "+firstErr(diags))
		return
	}

	opts := m.opts.EngineOptions.WithLimits(cfg.Limits)
	opts.Admission = m.admissionPool(opts.HighWatermark)
	opts.MetaStamps = map[string]any{
		"job_run_id":  jr.RunID,
		"job_trigger": jr.TriggerType,
	}
	if jr.ScheduledFor != "" {
		opts.MetaStamps["job_scheduled_for"] = jr.ScheduledFor
	}

	var sourceErr error
	opts.OnSourceError = func(node string, err error) {
		sourceErr = err
	}

	eng, err := engine.New(pip, m.st, m.reg, opts)
	if err != nil {
		fail(store.JobFailed, "run: engine: "+err.Error())
		return
	}
	runDone := make(chan error, 1)
	engineCtx, engineCancel := context.WithCancel(context.Background())
	defer engineCancel()
	go func() { runDone <- eng.Run(engineCtx) }()

	// Wait for quiescence: all sources stopped (exhausted or failed) and
	// nothing outstanding (committing phase).
	for !m.quiesced(eng) {
		select {
		case <-ctx.Done():
			// Canceled (overlap: latest, manager stop, or trigger ctx):
			// terminal-dead-letter the outstanding set (R2), then stop the
			// engine with a bounded wait — a wedged writer goroutine may
			// outlive the run by design (the process is "gone" from the
			// run's perspective; the store stays consistent).
			eng.Abandon("job canceled")
			eng.Close()
			engineCancel()
			select {
			case <-runDone:
			case <-time.After(opts.DrainTimeout + 2*time.Second):
			}
			jr.RowsRead = eng.Metrics.MessagesIn.Load()
			jr.Delivered = eng.Metrics.CommittedCount.Load() - eng.Metrics.DeadLettered.Load()
			jr.DeadLettered = eng.Metrics.DeadLettered.Load()
			fail(store.JobCanceled, "run canceled")
			return
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Sources done and committed: stop the engine gracefully.
	engineCancel()
	select {
	case <-runDone:
	case <-time.After(opts.DrainTimeout + 5*time.Second):
		eng.Close()
		<-runDone
	}

	jr.RowsRead = eng.Metrics.MessagesIn.Load()
	jr.Delivered = eng.Metrics.CommittedCount.Load() - eng.Metrics.DeadLettered.Load()
	jr.DeadLettered = eng.Metrics.DeadLettered.Load()
	if sourceErr != nil {
		fail(store.JobFailed, "source: "+sourceErr.Error())
		return
	}
	if jr.DeadLettered > 0 {
		fail(store.JobPartial, "")
		return
	}
	fail(store.JobSuccess, "")
}

func (m *Manager) quiesced(eng *engine.Engine) bool {
	return eng.Quiesced()
}

// admissionPool returns the shared spool admission pool, creating it on first
// use (capacity = the pipeline's resolved max_in_flight, defaulted the same
// way engine.New defaults it; same config every run, so first-run sizing is
// stable).
func (m *Manager) admissionPool(highWatermark int) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.admission == nil {
		if highWatermark <= 0 {
			highWatermark = engine.DefaultHighWatermark
		}
		m.admission = make(chan struct{}, highWatermark)
	}
	return m.admission
}

// sourceWatermarks reads each source's persisted {watermark} state.
func (m *Manager) sourceWatermarks(cfg *config.Pipeline) map[string]string {
	out := map[string]string{}
	for name := range cfg.Sources {
		state, _, err := m.st.SourceState(cfg.Name, name)
		if err != nil || len(state) == 0 {
			continue
		}
		var st struct {
			Watermark string `json:"watermark"`
		}
		if json.Unmarshal(state, &st) == nil && st.Watermark != "" {
			out[name] = st.Watermark
		}
	}
	return out
}

// resolveParameters applies trigger overrides onto declared defaults and
// validates everything against the declaration (§5.9 typing).
func (m *Manager) resolveParameters(trigger map[string]any, _ []string) (map[string]any, error) {
	resolved := map[string]any{}
	for name, spec := range m.cfg.Parameters {
		if spec == nil {
			continue
		}
		if v, ok := trigger[name]; ok {
			if err := validateParam(spec, v); err != nil {
				return nil, err
			}
			resolved[name] = v
			continue
		}
		if spec.Required {
			return nil, fmt.Errorf("jobs: parameter %q is required", name)
		}
		if spec.Default != nil {
			resolved[name] = spec.Default
		}
	}
	for name := range trigger {
		if _, declared := m.cfg.Parameters[name]; !declared {
			return nil, fmt.Errorf("jobs: unknown parameter %q (not declared in the pipeline)", name)
		}
	}
	return resolved, nil
}

func validateParam(spec *config.ParameterSpec, v any) error {
	if err := checkParamValue(spec, v); err != nil {
		return fmt.Errorf("jobs: parameter %q: %w", spec.Name, err)
	}
	return nil
}

// fireHook delivers the run summary to an inline hook sink (best effort,
// three attempts; hooks are notifications, not pipeline data — review R14).
func (m *Manager) fireHook(ctx context.Context, which string, jr *store.JobRun) {
	var hook *config.HookSink
	if m.cfg.Hooks == nil {
		return
	}
	if which == "failure" {
		hook = m.cfg.Hooks.Failure
	} else {
		hook = m.cfg.Hooks.Success
	}
	if hook == nil {
		return
	}
	sink, err := m.reg.NewSink(hook.Plugin, hook.PluginConfig)
	if err != nil {
		return
	}
	defer func() { _ = sink.Close() }()
	summary, _ := json.Marshal(map[string]any{
		"pipeline": jr.Pipeline, "run_id": jr.RunID, "status": jr.Status,
		"trigger": jr.TriggerType, "scheduled_for": jr.ScheduledFor,
		"rows_read": jr.RowsRead, "delivered": jr.Delivered,
		"dead_lettered": jr.DeadLettered, "error": jr.Error,
		"started_at": jr.StartedAt, "ended_at": jr.EndedAt,
	})
	msg := registry.Message{Out: append([]byte("\n"), summary...), Raw: summary, Codec: "json",
		Meta: map[string]any{"job_run_id": jr.RunID, "hook": which}}
	hookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			case <-hookCtx.Done():
				return
			}
		}
		if err := sink.Write(hookCtx, []registry.Message{msg}); err == nil {
			return
		}
	}
}

// applyRetention prunes finished run history past retention.history.
func (m *Manager) applyRetention() {
	if m.cfg.Run.Retention <= 0 {
		return
	}
	cutoff := m.opts.clock()().Add(-m.cfg.Run.Retention)
	_, _ = m.st.DeleteJobRunsBefore(m.cfg.Name, cutoff)
}

func auditParams(trigger map[string]any) map[string]any {
	if trigger == nil {
		return map[string]any{}
	}
	return trigger
}

func globalValues(resolved map[string]any, watermarks map[string]string) map[string]any {
	out := map[string]any{}
	for k, v := range resolved {
		if s, ok := v.(string); ok && s == "cursor" {
			// Global cursor = first source's watermark (single-source jobs
			// are the norm; documented for multi-source divergence).
			for _, wm := range watermarks {
				out[k] = wm
				break
			}
			continue
		}
		out[k] = v
	}
	return out
}

func firstErr(diags []config.Diagnostic) string {
	for _, d := range diags {
		if d.Severity == "error" {
			return d.Error()
		}
	}
	return "unknown error"
}

// checkParamValue validates one value against a declaration (types, enum,
// pattern, min/max; shared shape with config's load-time self-checks).
func checkParamValue(spec *config.ParameterSpec, v any) error {
	switch spec.Type {
	case "string":
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("want %s, got %T", spec.Type, v)
		}
		if len(spec.Enum) > 0 && !containsValue(spec.Enum, v) {
			return fmt.Errorf("value %q is not one of the declared enum values", s)
		}
		if spec.Pattern != "" {
			if re, err := regexp.Compile(spec.Pattern); err == nil && !re.MatchString(s) {
				return fmt.Errorf("value %q does not match pattern %q", s, spec.Pattern)
			}
		}
	case "integer":
		if !isInt(v) {
			return fmt.Errorf("want integer, got %T", v)
		}
	case "number":
		if !isNum(v) {
			return fmt.Errorf("want number, got %T", v)
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("want boolean, got %T", v)
		}
	}
	if isNum(v) {
		f := numOf(v)
		if spec.Min != nil && f < *spec.Min {
			return fmt.Errorf("value %v is below min %v", f, *spec.Min)
		}
		if spec.Max != nil && f > *spec.Max {
			return fmt.Errorf("value %v is above max %v", f, *spec.Max)
		}
	}
	return nil
}

func containsValue(list []any, v any) bool {
	for _, el := range list {
		switch a := v.(type) {
		case string:
			if b, ok := el.(string); ok && a == b {
				return true
			}
		case bool:
			if b, ok := el.(bool); ok && a == b {
				return true
			}
		default:
			if numOf(v) == numOf(el) {
				return true
			}
		}
	}
	return false
}

func isInt(v any) bool {
	switch t := v.(type) {
	case int, int64:
		return true
	case float64:
		return t == float64(int64(t))
	}
	return false
}

func isNum(v any) bool {
	switch v.(type) {
	case int, int64, float64:
		return true
	}
	return false
}

func numOf(v any) float64 {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case float64:
		return t
	}
	return 0
}
