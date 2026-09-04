package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/jobs"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/store"
)

// cmdTrigger manually fires a job pipeline once, optionally with parameters
// (backfill, §5.8). Standalone form: runs the job in this process against
// the durable store. (The daemon form arrives with the MCP/admin surface.)
func cmdTrigger(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("trigger", flag.ContinueOnError)
	configPath := fs.String("config", "", "job pipeline configuration file")
	parameters := fs.String("parameters", "", "JSON object of job parameters (e.g. backfill ranges)")
	dataDir := fs.String("data-dir", "data", "SQLite storage directory")
	ephemeral := fs.Bool("ephemeral", false, "in-memory store (nothing persists)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "trigger: --config is required")
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "trigger: unexpected argument %q (the pipeline comes from --config; unknown is an error)\n", fs.Arg(0))
		return 2
	}

	var params map[string]any
	if *parameters != "" {
		if err := json.Unmarshal([]byte(*parameters), &params); err != nil {
			fmt.Fprintf(os.Stderr, "trigger: --parameters is not valid JSON: %v\n", err)
			return 2
		}
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "trigger: builtin registration: %v\n", err)
		return 2
	}

	lr := config.LoadFile(*configPath)
	if lr.HasErrors() {
		printDiagsStderr(lr.Diagnostics)
		return 1
	}
	if !lr.Pipeline.IsJob() {
		fmt.Fprintf(os.Stderr, "trigger: pipeline %q is not a job pipeline (run.mode: job required)\n", lr.Pipeline.Name)
		return 1
	}
	if _, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil); hasErrDiagsCmd(diags) {
		printDiagsStderr(diags)
		return 1
	}

	var st store.Store
	if *ephemeral {
		st = store.NewMemory(lr.Pipeline.Name)
	} else {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "trigger: data dir: %v\n", err)
			return 2
		}
		sqlite, err := store.OpenSQLite(filepath.Join(*dataDir, "eventboat.db"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "trigger: open store: %v\n", err)
			return 2
		}
		st = sqlite
	}
	defer func() { _ = st.Close() }()

	opts := jobs.Options{}
	opts.EngineOptions = engine.DefaultOptions().WithLimits(lr.Pipeline.Limits)
	m, err := jobs.New(lr.Pipeline, *configPath, st, reg, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trigger: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "trigger: %v\n", err)
		return 1
	}
	runID, jr, err := m.Trigger(ctx, params, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trigger: %v\n", err)
		return 1
	}
	m.Stop()

	if jsonOut {
		out, _ := json.Marshal(map[string]any{
			"run_id": runID, "pipeline": lr.Pipeline.Name,
			"status": jr.Status, "rows_read": jr.RowsRead,
			"delivered": jr.Delivered, "dead_lettered": jr.DeadLettered,
			"error": jr.Error, "started_at": jr.StartedAt, "ended_at": jr.EndedAt,
		})
		fmt.Println(string(out))
	} else {
		fmt.Printf("eventboat: run %s %s — rows=%d delivered=%d dead_lettered=%d\n",
			runID, jr.Status, jr.RowsRead, jr.Delivered, jr.DeadLettered)
		if jr.Error != "" {
			fmt.Printf("  error: %s\n", jr.Error)
		}
	}
	switch jr.Status {
	case "success":
		return 0
	case "partial":
		return 1
	default:
		return 1
	}
}

// cmdJobs lists run history or shows one run. Flags may appear before or
// after the positional argument (Go's flag package stops at the first
// positional, so this parses by hand).
func cmdJobs(args []string, jsonOut bool) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: eventboat jobs list --config <job.yaml> [--data-dir DIR] [--limit N] | eventboat jobs show <run-id> --config <job.yaml>")
		return 2
	}
	sub := args[0]
	if sub != "list" && sub != "show" {
		fmt.Fprintf(os.Stderr, "jobs: unknown subcommand %q (list | show)\n", sub)
		return 2
	}
	var configPath, dataDir string
	limit := 20
	var positional []string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		switch a {
		case "--config":
			if i+1 >= len(rest) {
				fmt.Fprintln(os.Stderr, "jobs: --config needs a value")
				return 2
			}
			i++
			configPath = rest[i]
		case "--data-dir":
			if i+1 >= len(rest) {
				fmt.Fprintln(os.Stderr, "jobs: --data-dir needs a value")
				return 2
			}
			i++
			dataDir = rest[i]
		case "--limit":
			if i+1 >= len(rest) {
				fmt.Fprintln(os.Stderr, "jobs: --limit needs a value")
				return 2
			}
			i++
			if n, err := strconv.Atoi(rest[i]); err == nil {
				limit = n
			} else {
				fmt.Fprintf(os.Stderr, "jobs: --limit %q is not a number\n", rest[i])
				return 2
			}
		case "--json":
			// handled globally
		default:
			positional = append(positional, a)
		}
	}
	if configPath == "" {
		fmt.Fprintln(os.Stderr, "jobs: --config is required (identifies the pipeline)")
		return 2
	}
	if dataDir == "" {
		dataDir = "data"
	}
	lr := config.LoadFile(configPath)
	if lr.HasErrors() {
		printDiagsStderr(lr.Diagnostics)
		return 1
	}
	name := lr.Pipeline.Name

	st, err := store.OpenSQLite(filepath.Join(dataDir, "eventboat.db"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "jobs: open store: %v\n", err)
		return 2
	}
	defer func() { _ = st.Close() }()

	switch sub {
	case "list":
		runs, err := st.JobRuns(name, limit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jobs: %v\n", err)
			return 1
		}
		if jsonOut {
			b, _ := json.Marshal(runs)
			fmt.Println(string(b))
			return 0
		}
		fmt.Printf("%-14s %-9s %-9s %-20s %6s %9s %6s %s\n", "RUN", "STATUS", "TRIGGER", "SCHEDULED_FOR", "ROWS", "DELIVERED", "DEAD", "STARTED")
		for _, jr := range runs {
			fmt.Printf("%-14s %-9s %-9s %-20s %6d %9d %6d %s\n",
				jr.RunID, jr.Status, jr.TriggerType, jr.ScheduledFor, jr.RowsRead, jr.Delivered, jr.DeadLettered,
				jr.StartedAt.Format("2006-01-02 15:04:05"))
		}
		return 0
	case "show":
		if len(positional) == 0 {
			fmt.Fprintln(os.Stderr, "jobs show: run-id is required")
			return 2
		}
		runID := positional[0]
		jr, err := st.GetJobRun(name, runID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jobs: %v\n", err)
			return 1
		}
		dls, _ := st.DeadLettersForRun(name, runID)
		if jsonOut {
			b, _ := json.Marshal(map[string]any{"run": jr, "dead_letters": dls})
			fmt.Println(string(b))
			return 0
		}
		fmt.Printf("run %s\n  pipeline: %s\n  status: %s\n  trigger: %s\n  scheduled_for: %s\n",
			jr.RunID, jr.Pipeline, jr.Status, jr.TriggerType, jr.ScheduledFor)
		fmt.Printf("  parameters: %s\n", mustJSON(jr.Parameters))
		fmt.Printf("  rows_read=%d delivered=%d dead_lettered=%d\n", jr.RowsRead, jr.Delivered, jr.DeadLettered)
		if jr.Error != "" {
			fmt.Printf("  error: %s\n", jr.Error)
		}
		fmt.Printf("  started: %s\n  ended: %s\n", jr.StartedAt, jr.EndedAt)
		for _, dl := range dls {
			fmt.Printf("  dead-letter #%d node=%s reason=%s\n", dl.ID, dl.Node, dl.Reason)
		}
		return 0
	default:
		return 2
	}
}

func hasErrDiagsCmd(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
