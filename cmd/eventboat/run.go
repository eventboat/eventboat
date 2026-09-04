package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

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

	// Job pipelines run under the jobs manager (scheduler + admission + run
	// history, §5.8); continuous pipelines run the plain engine. Telemetry
	// follows the Runtime config (OTLP push; the Prometheus exposition needs
	// the daemon surface: run --config-dir / mcp --http).
	rt, err := runtimecfg.Load(*runtimeFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}
	if *dataDir != "" {
		rt.Storage.DataDir = *dataDir
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

	if pip.Config.IsJob() {
		return runJobPipeline(*configPath, pip, reg, rt.Storage.DataDir, *ephemeral || rt.Storage.Ephemeral, jsonOut, observer)
	}

	var st store.Store
	if *ephemeral {
		st = store.NewMemory(pip.Config.Name)
	} else {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "run: data dir: %v\n", err)
			return 2
		}
		dbPath := filepath.Join(*dataDir, "eventboat.db")
		sqlite, err := store.OpenSQLite(dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: open store: %v\n", err)
			return 2
		}
		st = sqlite
	}

	engOpts := engine.DefaultOptions().WithLimits(pip.Config.Limits)
	engOpts.Obs = observer
	eng, err := engine.New(pip, st, reg, engOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if !jsonOut {
		fmt.Printf("eventboat: running pipeline %q (store: %s)\n", pip.Config.Name, storeLabel(*ephemeral, *dataDir))
	}
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(ctx) }()

	sigStatus := func() {
		outstanding, settledThrough, arrived := eng.SettleSnapshot()
		m := &eng.Metrics
		if jsonOut {
			b, _ := json.Marshal(map[string]any{
				"pipeline":        pip.Config.Name,
				"outstanding":     outstanding,
				"settled_through": settledThrough,
				"arrived_max":     arrived,
				"messages_in":     m.MessagesIn.Load(),
				"settled":         m.SettledCount.Load(),
				"checkpoint":      m.CheckpointPtr.Load(),
				"dead_lettered":   m.DeadLettered.Load(),
				"cel_eval_errors": m.CelEvalErrors.Load(),
				"no_match":        m.NoMatch.Load(),
				"retries":         m.Retries.Load(),
				"dlq_failures":    m.DlqFailures.Load(),
			})
			fmt.Println(string(b))
			return
		}
		fmt.Printf("eventboat: pipeline %q stopped: settledThrough=%d outstanding=%d arrivedMax=%d in=%d settled=%d deadLettered=%d celErrors=%d noMatch=%d retries=%d\n",
			pip.Config.Name, settledThrough, outstanding, arrived,
			m.MessagesIn.Load(), m.SettledCount.Load(), m.DeadLettered.Load(),
			m.CelEvalErrors.Load(), m.NoMatch.Load(), m.Retries.Load())
	}

	select {
	case err := <-runErr:
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: %v\n", err)
			sigStatus()
			_ = st.Close()
			return 1
		}
	case <-ctx.Done():
	}
	sigStatus()
	_ = st.Close()
	if !jsonOut {
		fmt.Println("eventboat: stopped")
	}
	return 0
}

func storeLabel(ephemeral bool, dataDir string) string {
	if ephemeral {
		return "ephemeral (in-memory)"
	}
	return dataDir + string(os.PathSeparator) + "eventboat.db (SQLite, WAL)"
}

// runJobPipeline executes a job pipeline under the jobs manager until the
// context is canceled: crash recovery of in-flight runs, catchup for missed
// schedule ticks, then the cron scheduler (§5.8).
func runJobPipeline(configPath string, pip *ir.Pipeline, reg *registry.Registry, dataDir string, ephemeral bool, jsonOut bool, observer *obs.Obs) int {
	var st store.Store
	if ephemeral {
		st = store.NewMemory(pip.Config.Name)
	} else {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "run: data dir: %v\n", err)
			return 2
		}
		sqlite, err := store.OpenSQLite(filepath.Join(dataDir, "eventboat.db"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: open store: %v\n", err)
			return 2
		}
		st = sqlite
	}
	defer func() { _ = st.Close() }()

	opts := jobs.Options{}
	opts.EngineOptions = engine.DefaultOptions().WithLimits(pip.Config.Limits)
	opts.EngineOptions.Obs = observer
	m, err := jobs.New(pip.Config, configPath, st, reg, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
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
			pip.Config.Name, schedule, pip.Config.Run.Overlap, storeLabel(ephemeral, dataDir))
	}
	<-ctx.Done()
	m.Stop()
	if !jsonOut {
		fmt.Println("eventboat: stopped")
	}
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
