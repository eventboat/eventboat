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
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/store"
)

func cmdRun(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	dataDir := fs.String("data-dir", "data", "SQLite storage directory (deployment-level concern, POC flag)")
	ephemeral := fs.Bool("ephemeral", false, "in-memory store: nothing persists across restarts")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "run: --config is required")
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
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions())
	for _, d := range diags {
		if d.Severity == "error" {
			printDiagsStderr(diags)
			fmt.Fprintln(os.Stderr, "run: pipeline failed verify")
			return 1
		}
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

	eng, err := engine.New(pip, st, reg, engine.DefaultOptions().WithLimits(pip.Config.Limits))
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
				"settled":         m.Settled.Load(),
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
			m.MessagesIn.Load(), m.Settled.Load(), m.DeadLettered.Load(),
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

func printDiagsStderr(diags []config.Diagnostic) {
	for _, d := range diags {
		fmt.Fprintln(os.Stderr, d.Error())
		if d.Hint != "" {
			fmt.Fprintf(os.Stderr, "    hint: %s\n", d.Hint)
		}
	}
}
