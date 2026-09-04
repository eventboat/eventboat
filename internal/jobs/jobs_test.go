package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// recordingSink records delivered bytes with optional wedging.
type recordingSink struct {
	mu     sync.Mutex
	raw    [][]byte
	block  func(attempt int) (<-chan struct{}, bool)
	writeN int
}

func (s *recordingSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	s.writeN++
	n := s.writeN
	s.mu.Unlock()
	if s.block != nil {
		if ch, ok := s.block(n); ok {
			<-ch // deliberate: not cancellable, models a wedged writer
		}
	}
	s.mu.Lock()
	for _, m := range msgs {
		s.raw = append(s.raw, m.Out)
	}
	s.mu.Unlock()
	return nil
}

func (s *recordingSink) Close() error { return nil }

func (s *recordingSink) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.raw...)
}

func (s *recordingSink) writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeN
}

// --- harness ---

type jharness struct {
	t        *testing.T
	reg      *registry.Registry
	sinks    map[string]*recordingSink
	sinkMu   sync.Mutex
	yamlPath string
}

const jobYAMLTemplate = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: nightly }
run:
  mode: job%s
parameters:
  from: { type: string, default: cursor }
  to:   { type: string, default: now }
hooks: %s
limits: { drain_timeout: 200ms }
sources:
  pull:
    decoder: json
    fakepull: { id: feed }
transforms:
  enrich:
    from: [pull]
    script: |
      payload.seen = True
sinks:
  out:
    from: [enrich]
    encoder: json
    memsink: { id: out }
`

func jobYAML(schedule, overlap, catchup string, skip bool, hooks string) string {
	run := ""
	if schedule != "" {
		run += fmt.Sprintf("\n  schedule: %q", schedule)
	}
	if overlap != "" {
		run += "\n  overlap: " + overlap
	}
	if catchup != "" && catchup != "0s" {
		run += "\n  catchup_window: " + catchup
	}
	if skip {
		run += "\n  skip_if_successful: true"
	}
	if hooks == "" {
		hooks = "{}"
	}
	return fmt.Sprintf(jobYAMLTemplate, run, hooks)
}

// memsink is a test sink plugin whose instances live in the harness.
const memSinkSchema = `{"type":"object","required":["id"],"properties":{"id":{"type":"string"}},"additionalProperties":false}`

func newJobHarness(t *testing.T, schedule, overlap, catchup string, skip bool, hooks string) *jharness {
	t.Helper()
	h := &jharness{t: t, reg: registry.New(), sinks: map[string]*recordingSink{}}
	if err := builtin.RegisterAll(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSink("memsink", 1, memSinkSchema, func(cfg map[string]any) (registry.Sink, error) {
		id, _ := cfg["id"].(string)
		h.sinkMu.Lock()
		defer h.sinkMu.Unlock()
		if s, ok := h.sinks[id]; ok {
			return s, nil
		}
		s := &recordingSink{}
		h.sinks[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	yamlText := jobYAML(schedule, overlap, catchup, skip, hooks)
	dir := t.TempDir()
	h.yamlPath = filepath.Join(dir, "pipeline.yaml")
	if err := writeFile(h.yamlPath, []byte(yamlText)); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *jharness) sink(id string) *recordingSink {
	h.sinkMu.Lock()
	defer h.sinkMu.Unlock()
	return h.sinks[id]
}

func (h *jharness) buildManager(st store.Store, clock func() time.Time) *Manager {
	h.t.Helper()
	lr := config.LoadFile(h.yamlPath)
	if lr.HasErrors() {
		h.t.Fatalf("config: %+v", lr.Diagnostics)
	}
	if _, diags := ir.Build(lr.Pipeline, h.reg, starhost.DefaultOptions(), nil); irHasErr(diags) {
		h.t.Fatalf("verify: %+v", diags)
	}
	opts := Options{Clock: clock, NewRunID: counterRunID()}
	opts.EngineOptions.BackoffBase = time.Millisecond
	opts.EngineOptions.DLBackoff = time.Millisecond
	opts.EngineOptions.BatchFlush = 5 * time.Millisecond
	opts.EngineOptions.DrainTimeout = 200 * time.Millisecond
	opts.EngineOptions.DefaultTimeout = 2 * time.Second
	m, err := New(lr.Pipeline, h.yamlPath, st, h.reg, opts)
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func counterRunID() func() string {
	var n int
	return func() string {
		n++
		return fmt.Sprintf("run-%03d", n)
	}
}

func irHasErr(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

// --- tests ---

// Manual trigger runs to success: rows read, delivered, watermark committed,
// history recorded with parameters.
func TestJobTriggerRunLifecycle(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "", "skip", "0s", false, "")
	st := store.NewMemory("nightly")
	m := h.buildManager(st, testkit.FixedClock(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)))

	feed := testkit.FakePull("feed")
	for i := 0; i < 5; i++ {
		feed.StageJSON(fmt.Sprintf(`{"i":%d}`, i), fmt.Sprintf("c%02d", i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runID, jr, err := m.Trigger(ctx, map[string]any{"to": "2026-09-04T00:00:00Z"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr == nil || jr.Status != store.JobSuccess {
		t.Fatalf("run %s status = %+v, want success", runID, jr)
	}
	if jr.RowsRead != 5 || jr.Delivered != 5 || jr.DeadLettered != 0 {
		t.Errorf("counts: %+v", jr)
	}
	if feed.Watermark() != "c04" {
		t.Errorf("watermark = %q, want c04", feed.Watermark())
	}
	runs, _ := st.JobRuns("nightly", 10)
	if len(runs) != 1 || runs[0].RunID != runID {
		t.Fatalf("history: %+v", runs)
	}
	if runs[0].Parameters["to"] != "2026-09-04T00:00:00Z" {
		t.Errorf("audit parameters = %+v (trigger-time intent)", runs[0].Parameters)
	}
	got := h.sink("out").snapshot()
	if len(got) != 5 {
		t.Fatalf("delivered %d messages, want 5", len(got))
	}
	m.Stop()
}

// The kill-9 specialized test (M2 acceptance): a wedged delivery mid-run
// "crashes" the process; a restarted manager resumes the same run from the
// spool replay + watermark and finishes it (invariants 3 & 7 combined).
func TestJobKill9ResumeFromWatermark(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "", "skip", "0s", false, "")

	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	st1, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })

	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) }) // release the wedged write after the test

	feed := testkit.FakePull("feed")
	for i := 0; i < 6; i++ {
		feed.StageJSON(fmt.Sprintf(`{"i":%d}`, i), fmt.Sprintf("c%02d", i))
	}

	// Manager 1 with a wedging sink: every write after the second wedges —
	// rows 0..1 settle, row 2 freezes mid-delivery (a process frozen mid-job).
	lr := config.LoadFile(h.yamlPath)
	if lr.HasErrors() {
		t.Fatal(lr.Diagnostics)
	}
	opts := Options{Clock: time.Now, NewRunID: counterRunID()}
	opts.EngineOptions.BackoffBase = time.Millisecond
	opts.EngineOptions.DLBackoff = time.Millisecond
	opts.EngineOptions.BatchFlush = 5 * time.Millisecond
	opts.EngineOptions.DrainTimeout = 200 * time.Millisecond
	opts.EngineOptions.DefaultTimeout = 2 * time.Second
	opts.EngineOptions.SinkWrapper = func(node string, s registry.Sink) registry.Sink {
		return &wedgedSink{inner: s, gate: gate, after: 2}
	}
	m1, err := New(lr.Pipeline, h.yamlPath, st1, h.reg, opts)
	if err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if err := m1.Start(ctx1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m1.Trigger(ctx1, nil, false); err != nil {
		t.Fatal(err)
	}

	// Wait until the crash point: two messages settled (checkpoint = 2).
	// Budgets carry 3x headroom and scale with EVENTBOAT_TEST_TIMEOUT_FACTOR:
	// this test keeps two live managers plus a wedged writer schedulable, and
	// under -count=N machine saturation fixed 5s/10s budgets flaked (the M4
	// observation item this hardening closes).
	waitFor(t, loadScaled(15*time.Second), func() bool {
		cp, err := st1.Checkpoint("nightly")
		return err == nil && cp == 2
	})

	// Simulate the crash: abandon manager 1 (never canceled — its wedged
	// write stays wedged exactly like a frozen process; no further store
	// writes happen) and open a "new process" on the same store.
	st2, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	m2 := h.buildManager(st2, time.Now)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := m2.Start(ctx2); err != nil {
		t.Fatal(err)
	}

	waitFor(t, loadScaled(30*time.Second), func() bool {
		runs, _ := st2.JobRuns("nightly", 5)
		for _, jr := range runs {
			if jr.Status == store.JobSuccess {
				return true
			}
		}
		return false
	})

	runs, _ := st2.JobRuns("nightly", 5)
	if len(runs) != 1 {
		t.Fatalf("resume must continue the SAME run, got %d runs", len(runs))
	}
	if runs[0].Status != store.JobSuccess {
		t.Fatalf("resumed run status = %s", runs[0].Status)
	}
	// at-least-once: every row delivered; the unsettled tail may appear
	// twice (spool replay + source re-pull), never zero times.
	delivered := map[string]bool{}
	for _, raw := range h.sink("out").snapshot() {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		delivered[fmt.Sprintf("%v", m["i"])] = true
	}
	for i := 0; i < 6; i++ {
		if !delivered[fmt.Sprintf("%d", i)] {
			t.Errorf("row %d never delivered after resume (delivered: %v)", i, delivered)
		}
	}
	// The durable watermark (what a NEXT restart would resume from) reached
	// the last row. Asserting the shared in-memory test double instead would
	// race with manager 1's cleanup-released goroutines, which also write it.
	waitFor(t, loadScaled(30*time.Second), func() bool {
		state, _, err := st2.SourceState("nightly", "pull")
		return err == nil && strings.Contains(string(state), `"watermark":"c05"`)
	})
	cancel2()
	m2.Stop()
	cancel1()
}

// overlap: skip rejects a second run while one is active.
func TestJobOverlapSkip(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "", "skip", "0s", false, "")
	st := store.NewMemory("nightly")
	m := h.buildManager(st, time.Now)

	gate := make(chan struct{})
	closeOnce := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(closeOnce)
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		return gate, true
	}
	feed := testkit.FakePull("feed")
	feed.StageJSON(`{"i":1}`, "c1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Start(ctx)
	if _, _, err := m.Trigger(ctx, nil, false); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(m.ActiveRuns()) == 1 })

	_, _, err := m.Trigger(ctx, nil, false)
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("second trigger under overlap:skip must be rejected, got %v", err)
	}
	closeOnce()
	waitFor(t, 5*time.Second, func() bool { return len(m.ActiveRuns()) == 0 })
	m.Stop()
}

// overlap: latest cancels the active run (canceled status, outstanding rows
// terminal-dead-lettered) and the new run completes.
func TestJobOverlapLatestCancelsAndReruns(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "", "latest", "0s", false, "")
	st := store.NewMemory("nightly")
	m := h.buildManager(st, time.Now)

	gate := make(chan struct{})
	closeOnce := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(closeOnce)
	// Wedge ONLY run 1's first write: the test observes the wedged attempt
	// before triggering the replacement. Wedging by attempt number alone is
	// racy — if run 1 is canceled before its sink fires, the replacement
	// engine's replay write becomes "attempt 1" and wedges instead.
	var passThrough atomic.Bool
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 1 && !passThrough.Load() {
			return gate, true
		}
		return nil, false
	}
	feed := testkit.FakePull("feed")
	feed.StageJSON(`{"i":1}`, "c1")
	feed.StageJSON(`{"i":2}`, "c2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Start(ctx)
	runID1, _, err := m.Trigger(ctx, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic cancel point: run 1 has a write wedged mid-delivery.
	waitFor(t, 5*time.Second, func() bool { return h.sink("out").writes() >= 1 })
	passThrough.Store(true)

	runID2, _, err := m.Trigger(ctx, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		jr, err := st.GetJobRun("nightly", runID2)
		return err == nil && jr.Status == store.JobSuccess
	})
	// The canceled run's terminal state lands after its bounded drain.
	waitFor(t, 10*time.Second, func() bool {
		jr, err := st.GetJobRun("nightly", runID1)
		return err == nil && (jr.Status == store.JobCanceled || jr.Status == store.JobFailed)
	})
	runs, _ := st.JobRuns("nightly", 10)
	statuses := map[string]string{}
	for _, jr := range runs {
		statuses[jr.RunID] = jr.Status
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %+v", runs)
	}
	if statuses[runID1] != store.JobCanceled {
		t.Errorf("canceled run status = %s, want canceled", statuses[runID1])
	}
	m.Stop()
}

// Parameters flow into strings (${parameters.x}) and script bindings; a
// backfill trigger overrides defaults (§5.9).
func TestJobParameterBackfill(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	yamlText := `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: backfill }
run: { mode: job }
parameters:
  region: { type: string, default: eu }
  floor:  { type: integer, default: 0 }
sources:
  pull:
    decoder: json
    fakepull: { id: feed }
transforms:
  t:
    from: [pull]
    script: |
      payload.region = parameters.region
      payload.above = payload.i >= parameters.floor
sinks:
  out:
    from: [t]
    memsink: { id: "out-${parameters.region}" }
`
	path := filepath.Join(dir, "p.yaml")
	if err := writeFile(path, []byte(yamlText)); err != nil {
		t.Fatal(err)
	}
	h := &jharness{t: t, reg: registry.New(), sinks: map[string]*recordingSink{}, yamlPath: path}
	if err := builtin.RegisterAll(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSink("memsink", 1, memSinkSchema, func(cfg map[string]any) (registry.Sink, error) {
		id, _ := cfg["id"].(string)
		h.sinkMu.Lock()
		defer h.sinkMu.Unlock()
		if s, ok := h.sinks[id]; ok {
			return s, nil
		}
		s := &recordingSink{}
		h.sinks[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}

	testkit.FakePull("feed").StageJSON(`{"i":5}`, "c1")

	st := store.NewMemory("backfill")
	m := h.buildManager(st, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Start(ctx)
	_, jr, err := m.Trigger(ctx, map[string]any{"region": "us", "floor": 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr.Status != store.JobSuccess {
		t.Fatalf("status = %s (%s)", jr.Status, jr.Error)
	}
	// ${parameters.region} substitution picked the right sink instance.
	if h.sink("out-us") == nil || len(h.sink("out-us").snapshot()) != 1 {
		t.Fatalf("substituted sink out-us not written: %+v", h.sinks)
	}
	var payload map[string]any
	_ = json.Unmarshal(h.sink("out-us").snapshot()[0], &payload)
	if payload["region"] != "us" || payload["above"] != true {
		t.Errorf("script bindings wrong: %+v", payload)
	}
	// Validation rejects bad types and unknown names.
	if _, _, err := m.Trigger(ctx, map[string]any{"floor": "abc"}, false); err == nil {
		t.Error("type-violating parameter accepted")
	}
	if _, _, err := m.Trigger(ctx, map[string]any{"nope": 1}, false); err == nil {
		t.Error("undeclared parameter accepted")
	}
	m.Stop()
}

// catchup_window: a missed tick inside the window runs once at startup;
// ticks outside the window are counted and skipped.
func TestJobCatchupWindow(t *testing.T) {
	testkit.ResetFakePull()
	// Every-minute schedule; the process was down across several ticks.
	// Window 2m: with last successful tick 12:25 and now 12:30, the missed
	// ticks are 12:26..12:30 — 12:26/12:27 fall outside the window (skipped,
	// counted), 12:28..12:30 are inside, and only the LATEST (12:30) runs.
	h := newJobHarness(t, "* * * * *", "skip", "2m", false, "")
	st := store.NewMemory("nightly")

	now := time.Date(2026, 9, 3, 12, 30, 0, 0, time.UTC)
	// Pretend a run for the 12:25 tick succeeded before the outage.
	if err := st.CreateJobRun(store.JobRun{
		RunID: "old", Pipeline: "nightly", Status: store.JobSuccess,
		TriggerType: "schedule", ScheduledFor: "2026-09-03T12:25:00Z",
	}); err != nil {
		t.Fatal(err)
	}

	var skipped int64
	m := h.buildManager(st, func() time.Time { return now })
	m.opts.CatchupTicksSkipped = func(d int64) { skipped += d }

	testkit.FakePull("feed").StageJSON(`{"i":1}`, "c1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		runs, _ := st.JobRuns("nightly", 10)
		return len(runs) >= 2
	})
	runs, _ := st.JobRuns("nightly", 10)
	var catchups int
	for _, jr := range runs {
		if jr.TriggerType == "catchup" {
			catchups++
			if jr.ScheduledFor != "2026-09-03T12:30:00Z" {
				t.Errorf("catchup tick = %s, want the latest in-window tick 12:30", jr.ScheduledFor)
			}
		}
	}
	if catchups != 1 {
		t.Errorf("catchup runs = %d, want exactly 1 (latest missed tick)", catchups)
	}
	if skipped != 2 {
		t.Errorf("out-of-window ticks skipped = %d, want 2 (12:26, 12:27)", skipped)
	}
	m.Stop()
}

// skip_if_successful: a tick that already succeeded does not run again.
func TestJobSkipIfSuccessful(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "* * * * *", "skip", "0s", true, "")
	st := store.NewMemory("nightly")
	// Now is 12:29:50: the scheduler's next activation is the 12:30 tick,
	// which already has a successful run — it must be skipped.
	now := time.Date(2026, 9, 3, 12, 29, 50, 0, time.UTC)
	if err := st.CreateJobRun(store.JobRun{
		RunID: "done", Pipeline: "nightly", Status: store.JobSuccess,
		TriggerType: "schedule", ScheduledFor: "2026-09-03T12:30:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	m := h.buildManager(st, func() time.Time { return now })
	testkit.FakePull("feed").StageJSON(`{"i":1}`, "c1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	runs, _ := st.JobRuns("nightly", 10)
	if len(runs) != 1 {
		t.Fatalf("skip_if_successful re-ran a succeeded tick: %+v", runs)
	}
	m.Stop()
}

// Source failure → run failed (distinct from partial); failure hook fires.
func TestJobSourceFailureFailsRunAndFiresHook(t *testing.T) {
	testkit.ResetFakePull()
	var hookBody = make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		select {
		case hookBody <- body:
		default:
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	h := newJobHarness(t, "", "skip", "0s", false,
		fmt.Sprintf("{ failure: { http: { url: %q } } }", srv.URL))
	st := store.NewMemory("nightly")
	m := h.buildManager(st, time.Now)

	feed := testkit.FakePull("feed")
	feed.StageJSON(`{"i":1}`, "c1")
	feed.FailNext()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Start(ctx)
	_, jr, err := m.Trigger(ctx, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr == nil || jr.Status != store.JobFailed {
		t.Fatalf("status = %+v, want failed", jr)
	}
	if !strings.Contains(jr.Error, "injected source failure") {
		t.Errorf("error = %q", jr.Error)
	}
	select {
	case b := <-hookBody:
		if !strings.Contains(string(b), `"status":"failed"`) {
			t.Errorf("hook payload = %s", b)
		}
	case <-time.After(2 * time.Second):
		t.Error("failure hook never fired")
	}
	m.Stop()
}

// Dead letters during the run → partial status (completed, needs attention).
func TestJobPartialOnDeadLetters(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	yamlText := `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: partial }
run: { mode: job }
sources:
  pull:
    decoder: json
    fakepull: { id: feed }
transforms:
  t:
    from: [pull]
    script: |
      if payload.i == 1:
          fail("boom")
sinks:
  out: { from: [t], memsink: { id: out } }
`
	path := filepath.Join(dir, "p.yaml")
	_ = writeFile(path, []byte(yamlText))
	h := &jharness{t: t, reg: registry.New(), sinks: map[string]*recordingSink{}, yamlPath: path}
	if err := builtin.RegisterAll(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSink("memsink", 1, memSinkSchema, func(cfg map[string]any) (registry.Sink, error) {
		id, _ := cfg["id"].(string)
		h.sinkMu.Lock()
		defer h.sinkMu.Unlock()
		if s, ok := h.sinks[id]; ok {
			return s, nil
		}
		s := &recordingSink{}
		h.sinks[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}

	feed := testkit.FakePull("feed")
	feed.StageJSON(`{"i":0}`, "c0")
	feed.StageJSON(`{"i":1}`, "c1") // script fails → dead letter

	st := store.NewMemory("partial")
	m := h.buildManager(st, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = m.Start(ctx)
	_, jr, err := m.Trigger(ctx, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr.Status != store.JobPartial {
		t.Fatalf("status = %s, want partial", jr.Status)
	}
	if jr.DeadLettered != 1 || jr.Delivered != 1 {
		t.Errorf("counts: %+v", jr)
	}
	dls, _ := st.DeadLettersForRun("partial", jr.RunID)
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "boom") {
		t.Fatalf("run-attributed dead letters: %+v", dls)
	}
	if dls[0].Backtrace == "" {
		t.Error("dead letter lost the Starlark backtrace")
	}
	m.Stop()
}

// --- helpers ---

// The pipeline-aggregated admission pool (M2 review R17): every concurrent
// run's engine draws from ONE shared semaphore, so max_in_flight is a
// pipeline total under overlap:all rather than per-run. The pool's capacity
// is resolved once from the first run's limits (same pipeline config every
// run). The gating mechanism itself is locked engine-side
// (TestSharedAdmissionPoolCapsConcurrentEngines); FakePull's per-Pull state
// reset makes a concurrent two-pull integration test unsound with the shared
// test double, so the wiring is asserted here.
func TestAdmissionPoolSharedAndStable(t *testing.T) {
	testkit.ResetFakePull()
	h := newJobHarness(t, "", "all", "0s", false, "")
	m := h.buildManager(store.NewMemory("nightly"), time.Now)

	first := m.admissionPool(4)
	second := m.admissionPool(1000)
	if first != second {
		t.Fatal("admissionPool must return the SAME pool across calls")
	}
	if cap(first) != 4 {
		t.Fatalf("pool capacity = %d, want 4 (first sizing wins)", cap(first))
	}
	if m.admissionPool(0) != first || cap(first) != 4 {
		t.Fatalf("pool capacity = %d after zero-watermark probe, want 4 (stable)", cap(first))
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// loadScaled stretches a wait budget on loaded machines (CI runners under
// -count=N package saturation): EVENTBOAT_TEST_TIMEOUT_FACTOR (float, default
// 1) multiplies the base budget. Conditions still poll at 5ms — only the
// failure budget moves.
func loadScaled(base time.Duration) time.Duration {
	f := 1.0
	if v := os.Getenv("EVENTBOAT_TEST_TIMEOUT_FACTOR"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed > 0 && parsed < 1000 {
			f = parsed
		}
	}
	return time.Duration(float64(base) * f)
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

// wedgedSink wraps a real sink, wedging writes after N attempts (kill-9
// simulation: the write hangs forever, like a process frozen mid-delivery).
type wedgedSink struct {
	inner  registry.Sink
	gate   <-chan struct{}
	after  int
	mu     sync.Mutex
	writes int
}

func (w *wedgedSink) Write(ctx context.Context, msgs []registry.Message) error {
	w.mu.Lock()
	w.writes++
	n := w.writes
	w.mu.Unlock()
	if n > w.after {
		<-w.gate // deliberate: not cancellable
	}
	return w.inner.Write(ctx, msgs)
}

func (w *wedgedSink) Close() error { return w.inner.Close() }
