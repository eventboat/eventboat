package ops

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// --- candidate 08 acceptance: drain/stop semantics, state machine, status ---

// blockingSink wedges the first delivery until the gate closes: a run stuck
// mid-flight is exactly what Drain/Deploy must wait for.
type blockingSink struct {
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (s *blockingSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.once.Do(func() { close(s.entered) })
	<-s.gate
	return nil
}

func (s *blockingSink) Close() error { return nil }

func lifecycleRegistry(t *testing.T) (*registry.Registry, *blockingSink) {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	sink := &blockingSink{entered: make(chan struct{}), gate: make(chan struct{})}
	schema := `{"type":"object","properties":{},"additionalProperties":false}`
	if err := reg.RegisterSink("blocksink", 1, schema, func(map[string]any) (registry.Sink, error) {
		return sink, nil
	}); err != nil {
		t.Fatal(err)
	}
	return reg, sink
}

const lifecycleYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lifecycle-job }
run: { mode: job }
limits: { drain_timeout: 200ms }
sources:
  in:
    decoder: json
    fakepull: { id: lifecycle-feed }
sinks:
  out:
    depends_on: [in]
    blocksink: {}
`

func pipelineStatus(t *testing.T, svc *Service, name string) PipelineStatus {
	t.Helper()
	for _, st := range svc.Status() {
		if st.Pipeline == name {
			return st
		}
	}
	t.Fatalf("pipeline %q not in status", name)
	return PipelineStatus{}
}

func newLifecycleService(t *testing.T, reg *registry.Registry) (*Service, *store.Owner) {
	t.Helper()
	owner := store.NewMemoryOwner()
	svc := New(Options{DataDir: t.TempDir(), Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })
	return svc, owner
}

// Candidate 08 acceptance 2 (ops half): a lease held by ANOTHER process makes
// Deploy fail loudly — the daemon must not start a second engine on a store
// someone else is writing.
func TestDeployRefusedWhenLeaseHeldElsewhere(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	// The "other process" holds the pipeline's lease for the whole test.
	holder := store.NewOwner(dir)
	lease, err := holder.Acquire("lease-held-job")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Release() }()
	defer func() { _ = holder.Close() }()

	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	owner := store.NewOwner(dir)
	svc := New(Options{DataDir: dir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })

	cfg := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lease-held-job }
run: { mode: job }
sources:
  in:
    decoder: json
    fakepull: { id: lease-feed }
sinks:
  out:
    depends_on: [in]
    file: { path: out.jsonl }
`
	_, err = svc.Deploy(context.Background(), cfg)
	if err == nil {
		t.Fatal("Deploy succeeded while another process held the store lease")
	}
	for _, want := range []string{"lease", "admin/MCP"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Deploy refusal does not mention %q: %v", want, err)
		}
	}
	if _, err := svc.of("lease-held-job"); err == nil {
		t.Fatal("failed Deploy left the pipeline deployed")
	}
}

// Candidate 08 acceptance 3 (ops half): Drain returns only after every run
// persisted its terminal state; the status is `drained` (not a fake
// `running`), Pause on drained is refused by the transition table, and
// Resume from drained RESTARTS the pipeline — a real, live instance.
func TestDrainWaitsAndResumeRestarts(t *testing.T) {
	testkit.ResetFakePull()
	reg, sink := lifecycleRegistry(t)
	svc, owner := newLifecycleService(t, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := svc.Deploy(ctx, lifecycleYAML); err != nil {
		t.Fatal(err)
	}
	st, err := owner.Open("lifecycle-job")
	if err != nil {
		t.Fatal(err)
	}
	testkit.FakePull("lifecycle-feed").StageJSON(`{"i":1}`, "c1")
	if _, err := svc.Trigger(ctx, "lifecycle-job", nil, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("run never reached the sink")
	}

	release := sync.OnceFunc(func() { close(sink.gate) })
	t.Cleanup(release)

	if err := svc.Drain("lifecycle-job"); err != nil {
		t.Fatal(err)
	}
	runs, err := st.JobRuns("lifecycle-job", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs after Drain = %+v (%v), want exactly the drained run", runs, err)
	}
	if store.IsRunnableStatus(runs[0].Status) {
		t.Fatalf("run status after Drain = %s, want terminal (Stop waits)", runs[0].Status)
	}
	if runnable, err := st.RunnableJobRuns("lifecycle-job"); err != nil || len(runnable) != 0 {
		t.Fatalf("runnable runs after Drain = %+v (%v), want none", runnable, err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != stateDrained {
		t.Fatalf("status after Drain = %q, want %q", got, stateDrained)
	}
	// Draining an already-drained pipeline is an idempotent no-op; pausing it
	// is not a transition and must be refused (no fake state).
	if err := svc.Drain("lifecycle-job"); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if err := svc.Pause("lifecycle-job"); err == nil || !strings.Contains(err.Error(), "drained") {
		t.Fatalf("Pause on drained error = %v, want a transition refusal naming drained", err)
	}

	// Resume from drained restarts: a live instance processes new input.
	release()
	if err := svc.Resume(ctx, "lifecycle-job"); err != nil {
		t.Fatal(err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != stateRunning {
		t.Fatalf("status after Resume = %q, want %q", got, stateRunning)
	}
	testkit.FakePull("lifecycle-feed").StageJSON(`{"i":2}`, "c2")
	jr, err := svc.Trigger(ctx, "lifecycle-job", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr.Status != store.JobSuccess {
		t.Fatalf("run on the restarted instance = %s (%s), want success", jr.Status, jr.Error)
	}

	// Pause → Resume round-trips too, with Pause idempotent.
	if err := svc.Pause("lifecycle-job"); err != nil {
		t.Fatal(err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != statePaused {
		t.Fatalf("status after Pause = %q, want %q", got, statePaused)
	}
	if err := svc.Pause("lifecycle-job"); err != nil {
		t.Fatalf("second Pause: %v", err)
	}
	if err := svc.Resume(ctx, "lifecycle-job"); err != nil {
		t.Fatal(err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != stateRunning {
		t.Fatalf("status after Resume from paused = %q, want %q", got, stateRunning)
	}
}

// Candidate 08 acceptance 3 (ops half): Deploy waits for the previous
// instance to stop — every run terminal — before starting the replacement,
// so the replacement's crash recovery finds nothing to resume and cannot
// double-run.
func TestDeployWaitsForUnterminalRuns(t *testing.T) {
	testkit.ResetFakePull()
	reg, sink := lifecycleRegistry(t)
	svc, owner := newLifecycleService(t, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := svc.Deploy(ctx, lifecycleYAML); err != nil {
		t.Fatal(err)
	}
	st, err := owner.Open("lifecycle-job")
	if err != nil {
		t.Fatal(err)
	}
	testkit.FakePull("lifecycle-feed").StageJSON(`{"i":1}`, "c1")
	if _, err := svc.Trigger(ctx, "lifecycle-job", nil, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("run never reached the sink")
	}

	release := sync.OnceFunc(func() { close(sink.gate) })
	t.Cleanup(release)

	if _, err := svc.Deploy(ctx, lifecycleYAML); err != nil {
		t.Fatal(err)
	}
	runs, err := st.JobRuns("lifecycle-job", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs after replacement Deploy = %+v (%v), want the one original run", runs, err)
	}
	if store.IsRunnableStatus(runs[0].Status) {
		t.Fatalf("run status after replacement Deploy = %s, want terminal", runs[0].Status)
	}
	if runnable, err := st.RunnableJobRuns("lifecycle-job"); err != nil || len(runnable) != 0 {
		t.Fatalf("runnable runs after replacement Deploy = %+v (%v), want none", runnable, err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != stateRunning {
		t.Fatalf("status after replacement Deploy = %q, want the fresh instance running", got)
	}
	release()
}

// Candidate 08 acceptance 3 (ops half): Pause has the same terminal barrier
// as Drain — no runnable run survives it.
func TestPauseWaitsForTerminalRuns(t *testing.T) {
	testkit.ResetFakePull()
	reg, sink := lifecycleRegistry(t)
	svc, owner := newLifecycleService(t, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := svc.Deploy(ctx, lifecycleYAML); err != nil {
		t.Fatal(err)
	}
	st, err := owner.Open("lifecycle-job")
	if err != nil {
		t.Fatal(err)
	}
	testkit.FakePull("lifecycle-feed").StageJSON(`{"i":1}`, "c1")
	if _, err := svc.Trigger(ctx, "lifecycle-job", nil, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("run never reached the sink")
	}
	release := sync.OnceFunc(func() { close(sink.gate) })
	t.Cleanup(release)

	if err := svc.Pause("lifecycle-job"); err != nil {
		t.Fatal(err)
	}
	if runnable, err := st.RunnableJobRuns("lifecycle-job"); err != nil || len(runnable) != 0 {
		t.Fatalf("runnable runs after Pause = %+v (%v), want none", runnable, err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != statePaused {
		t.Fatalf("status after Pause = %q, want %q", got, statePaused)
	}
	release()
}

// Candidate 08 acceptance 4: completed/failed are terminal for one instance —
// Pause/Drain/Resume are refused with a clear error and a Deploy replaces it.
func TestTerminalInstanceRefusesLifecycle(t *testing.T) {
	dir := t.TempDir()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner := store.NewMemoryOwner()
	svc := New(Options{DataDir: dir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"i\":1}\n{\"i\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := batchYAML("batch-term", input, filepath.Join(dir, "out.jsonl"))
	if _, err := svc.Deploy(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	waitForBatchStatus(t, svc, "batch-term", stateCompleted, false)

	if err := svc.Pause("batch-term"); err == nil || !strings.Contains(err.Error(), "completed") {
		t.Fatalf("Pause on a completed pipeline error = %v, want a terminal refusal", err)
	}
	if err := svc.Drain("batch-term"); err == nil || !strings.Contains(err.Error(), "completed") {
		t.Fatalf("Drain on a completed pipeline error = %v, want a terminal refusal", err)
	}
	if err := svc.Resume(ctx, "batch-term"); err == nil || !strings.Contains(err.Error(), "completed") {
		t.Fatalf("Resume on a completed pipeline error = %v, want a terminal refusal", err)
	}
	// A Deploy replaces the terminal instance (a fresh one starts).
	if _, err := svc.Deploy(ctx, cfg); err != nil {
		t.Fatalf("Deploy over a completed pipeline: %v", err)
	}
	waitForBatchStatus(t, svc, "batch-term", stateCompleted, false)
}

// Candidate 08 acceptance 5 (ops half): with several runnable runs, Status
// reports the LATEST one — the old loop let the oldest overwrite the newest.
func TestStatusReportsLatestRunnableRun(t *testing.T) {
	testkit.ResetFakePull()
	reg, _ := lifecycleRegistry(t)
	svc, owner := newLifecycleService(t, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := svc.Deploy(ctx, lifecycleYAML); err != nil {
		t.Fatal(err)
	}
	st, err := owner.Open("lifecycle-job")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	oldPending := store.JobRun{RunID: "old-pending", Pipeline: "lifecycle-job", Status: store.JobPending, TriggerType: "manual", StartedAt: base}
	newRunning := store.JobRun{RunID: "new-running", Pipeline: "lifecycle-job", Status: store.JobRunning, TriggerType: "manual", StartedAt: base.Add(time.Second)}
	if err := st.CreateJobRun(oldPending); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJobRun(newRunning); err != nil {
		t.Fatal(err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != "run:"+store.JobRunning {
		t.Fatalf("status with two runnable runs = %q, want %q (the latest run)", got, "run:"+store.JobRunning)
	}

	// Once only terminal runs remain, the instance status stands.
	newRunning.Status = store.JobSuccess
	newRunning.EndedAt = base.Add(2 * time.Second)
	if err := st.UpdateJobRun(newRunning); err != nil {
		t.Fatal(err)
	}
	oldPending.Status = store.JobCanceled
	oldPending.EndedAt = base.Add(2 * time.Second)
	if err := st.UpdateJobRun(oldPending); err != nil {
		t.Fatal(err)
	}
	if got := pipelineStatus(t, svc, "lifecycle-job").Status; got != stateRunning {
		t.Fatalf("status with only terminal runs = %q, want %q", got, stateRunning)
	}
}
