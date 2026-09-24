package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/jobs"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/runtimecfg"
	"github.com/eventboat/eventboat/internal/store"
)

func cmdRun(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	configDir := fs.String("config-dir", "", "directory of pipeline YAML files (multi-pipeline daemon with admin surface)")
	runtimeFile := fs.String("runtime", "", "Runtime configuration file (telemetry endpoints; default: ./eventboat.yaml)")
	dataDir := fs.String("data-dir", "data", "SQLite storage directory (deployment-level concern, POC flag)")
	ephemeral := fs.Bool("ephemeral", false, "in-memory store: nothing persists across restarts")
	_ = fs.String("admin-token", "", "bearer token for the admin/MCP HTTP surface (required for non-loopback binds; directory mode only)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configDir != "" {
		return cmdRunDir(args, jsonOut)
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "run: --config or --config-dir is required")
		return 2
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: builtin registration: %v\n", err)
		return 2
	}

	lr := config.LoadFile(*configPath)
	if lr.HasErrors() {
		printDiagsStderr(lr.Diagnostics)
		fmt.Fprintln(os.Stderr, "run: pipeline failed verify (run verify for details)")
		return 1
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	for _, d := range diags {
		if d.Severity == "error" {
			printDiagsStderr(diags)
			fmt.Fprintln(os.Stderr, "run: pipeline failed verify")
			return 1
		}
	}

	// Telemetry follows the Runtime config (OTLP push; the Prometheus
	// exposition needs the daemon surface: run --config-dir / mcp --http).
	rt, err := runtimecfg.Load(*runtimeFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}
	if *dataDir != "" {
		rt.Storage.DataDir = *dataDir
	}
	if *ephemeral {
		rt.Storage.Ephemeral = true
	}
	observer, err := obs.Setup(context.Background(), obs.Config{
		OTLPEndpoint: rt.Telemetry.OTLPEndpoint,
		SampleRatio:  rt.Telemetry.SampleRatio,
		Prometheus:   false, // no HTTP surface in single-pipeline mode
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: telemetry: %v\n", err)
		return 2
	}
	defer func() { _ = observer.Shutdown(context.Background()) }()

	// One store owner per process, from the runtime storage config: the
	// canonical per-pipeline file under <data-dir>/stores/ (or the cached
	// in-memory owner under --ephemeral). The pipeline is loaded before the
	// store opens, so a one-shot run reads the SAME file the daemon uses.
	owner := newStoreOwner(rt.Storage)
	defer func() { _ = owner.Close() }()
	// A one-shot run is an engine writer: hold the cross-process store lease
	// for its whole life, or refuse when another process (the daemon) owns
	// the pipeline (candidate 08).
	lease, err := acquireRunLease(owner, "run", pip.Config.Name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = lease.Release() }()
	st, err := owner.Open(pip.Config.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: open store: %v\n", err)
		return 2
	}

	// Job pipelines run under the jobs manager (scheduler + admission + run
	// history, §5.8); continuous pipelines run the plain engine.
	if pip.Config.IsJob() {
		return runJobPipeline(*configPath, pip, reg, st, rt.Storage, storeDesc(owner, rt.Storage.Ephemeral, pip.Config.Name), jsonOut, observer)
	}

	engOpts := engine.DefaultOptions().WithLimits(pip.Config.Limits)
	engOpts.Obs = observer
	engOpts.SpoolRetention = rt.Storage.SpoolRetention
	eng, err := engine.New(pip, st, reg, engOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if !jsonOut {
		fmt.Printf("eventboat: running pipeline %q (store: %s)\n", pip.Config.Name, storeDesc(owner, rt.Storage.Ephemeral, pip.Config.Name))
	}
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(ctx) }()

	sigStatus := func() {
		outstanding, committedThrough, arrived := eng.CommitSnapshot()
		m := &eng.Metrics
		if jsonOut {
			b, _ := json.Marshal(map[string]any{
				"pipeline":          pip.Config.Name,
				"outstanding":       outstanding,
				"committed_through": committedThrough,
				"arrived_max":       arrived,
				"messages_in":       m.MessagesIn.Load(),
				"committed":         m.CommittedCount.Load(),
				"checkpoint":        m.CheckpointPtr.Load(),
				"dead_lettered":     m.DeadLettered.Load(),
				"cel_eval_errors":   m.CelEvalErrors.Load(),
				"no_match":          m.NoMatch.Load(),
				"retries":           m.Retries.Load(),
				"dlq_failures":      m.DlqFailures.Load(),
			})
			fmt.Println(string(b))
			return
		}
		fmt.Printf("eventboat: pipeline %q stopped: committedThrough=%d outstanding=%d arrivedMax=%d in=%d committed=%d deadLettered=%d celErrors=%d noMatch=%d retries=%d\n",
			pip.Config.Name, committedThrough, outstanding, arrived,
			m.MessagesIn.Load(), m.CommittedCount.Load(), m.DeadLettered.Load(),
			m.CelEvalErrors.Load(), m.NoMatch.Load(), m.Retries.Load())
	}

	if pip.Config.IsBatch() {
		code := finishBatchRun(eng, ctx, cancel, runErr, sigStatus)
		if !jsonOut {
			fmt.Println("eventboat: stopped")
		}
		return code
	}

	// Continuous pipeline: a long-lived process. Wait blocks until the run
	// ends — SIGINT/SIGTERM (interrupted → 0), a self-stop on a source
	// failure or worker-fatal (failed → 1), or a finite source exhausting
	// (completed → 0).
	outcome := eng.Wait(ctx, runErr, engine.WaitOptions{})
	sigStatus()
	if outcome.Status == engine.RunFailed {
		reportRunFailure(outcome)
	}
	if !jsonOut {
		fmt.Println("eventboat: stopped")
	}
	return continuousExitCode(outcome)
}

// finishBatchRun drives a run.mode: batch pipeline to completion through the
// engine's single run-outcome decision point (candidate 02): Wait encapsulates
// quiesce, cancel and the bounded fold-in of a worker-fatal racing the quiesce
// poll; the exit code is the one-shot process contract — completed = 0,
// partial/failed/interrupted = 1.
func finishBatchRun(eng *engine.Engine, ctx context.Context, cancel context.CancelFunc, runErr <-chan error, sigStatus func()) int {
	outcome := eng.Wait(ctx, runErr, engine.WaitOptions{})
	cancel() // release the signal handler (idempotent)
	sigStatus()
	if outcome.Status == engine.RunFailed {
		reportRunFailure(outcome)
	}
	return batchExitCode(outcome)
}

// batchExitCode maps a batch run outcome onto the one-shot process contract:
// completed = 0; partial, failed and interrupted = 1 (a one-shot run that did
// not complete exits non-zero).
func batchExitCode(o engine.Outcome) int {
	if o.Status == engine.RunCompleted {
		return 0
	}
	return 1
}

// continuousExitCode maps a long-lived run outcome onto its process contract:
// only a failed run (worker-fatal or source failure) exits non-zero; a
// graceful SIGINT/SIGTERM stop (interrupted) and a self-completed run exit
// zero.
func continuousExitCode(o engine.Outcome) int {
	if o.Status == engine.RunFailed {
		return 1
	}
	return 0
}

// reportRunFailure prints a failed outcome's cause (worker-fatal first, then
// the first source error in stable node order).
func reportRunFailure(o engine.Outcome) {
	if text := o.FailureText(); text != "" {
		fmt.Fprintf(os.Stderr, "run: %s\n", text)
	}
}

// runJobPipeline executes a job pipeline under the jobs manager until the
// context is canceled: crash recovery of in-flight runs, catchup for missed
// schedule ticks, then the cron scheduler (§5.8). The store handle comes from
// the caller's owner (same file as every other entry point); the caller owns
// its lifetime.
func runJobPipeline(configPath string, pip *ir.Pipeline, reg *registry.Registry, st store.Store, storage runtimecfg.Storage, storePath string, jsonOut bool, observer *obs.Obs) int {
	opts := jobs.Options{}
	opts.EngineOptions = engine.DefaultOptions().WithLimits(pip.Config.Limits)
	opts.EngineOptions.Obs = observer
	opts.EngineOptions.SpoolRetention = storage.SpoolRetention
	m, err := jobs.New(pip.Config, configPath, st, reg, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 1
	}
	if !jsonOut {
		schedule := pip.Config.Run.Schedule
		if schedule == "" {
			schedule = "manual/trigger only"
		}
		fmt.Printf("eventboat: job pipeline %q (schedule: %s, overlap: %s, store: %s)\n",
			pip.Config.Name, schedule, pip.Config.Run.Overlap, storePath)
	}
	<-ctx.Done()
	m.Stop()
	if !jsonOut {
		fmt.Println("eventboat: stopped")
	}
	// The long-lived scheduler contract: SIGTERM/SIGINT is a graceful stop,
	// exit 0. A failed job run is recorded in run history; it never decides
	// this process's exit code (candidate 02 keeps the mapping unchanged).
	return 0
}

func printDiagsStderr(diags []config.Diagnostic) {
	for _, d := range diags {
		fmt.Fprintln(os.Stderr, d.Error())
		if d.Hint != "" {
			fmt.Fprintf(os.Stderr, "    hint: %s\n", d.Hint)
		}
	}
}
