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
	"strings"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/explain"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// cmdExplain renders the deterministic pipeline walkthrough: symbolic by
// default, message-level with --message (CEL edges really evaluated,
// Starlark scripts really dry-run — the sandbox is deterministic), plus
// --topology for mermaid + ASCII renderings (§3.3).
func cmdExplain(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	message := fs.String("message", "", "sample message JSON file (message-level evaluation)")
	entry := fs.String("at", "", "entry node for the sample message (default: first source)")
	topology := fs.Bool("topology", false, "render the DAG (mermaid + ASCII) instead of a trace")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "explain: --config is required")
		return 2
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "explain: %v\n", err)
		return 2
	}
	lr := config.LoadFile(*configPath)
	if lr.HasErrors() {
		printDiagsStderr(lr.Diagnostics)
		return 1
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		printDiagsStderr(diags)
		return 1
	}

	if *topology {
		if jsonOut {
			b, _ := json.Marshal(map[string]string{"mermaid": explain.TopologyMermaid(pip), "ascii": explain.TopologyASCII(pip)})
			fmt.Println(string(b))
		} else {
			fmt.Println(explain.TopologyMermaid(pip))
			fmt.Println()
			fmt.Print(explain.TopologyASCII(pip))
		}
		return 0
	}

	opts := explain.Options{EntryNode: *entry}
	if *message != "" {
		raw, err := os.ReadFile(*message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "explain: read message: %v\n", err)
			return 2
		}
		opts.Message = raw
	}
	trace, err := explain.Trace(pip, opts)
	if err != nil && trace == "" {
		fmt.Fprintf(os.Stderr, "explain: %v\n", err)
		return 2
	}
	fmt.Print(trace)
	return 0
}

// cmdReplay re-injects dead letters, spool windows or one job run's dead
// letters into a live pipeline (§3.3). --dry-run explains instead of
// delivering.
func cmdReplay(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	dlq := fs.Bool("dlq", false, "replay dead letters (filtered by --since/--where)")
	spoolMode := fs.Bool("spool", false, "replay a spool window from --from <seq>")
	jobRun := fs.String("job", "", "replay one job run's dead letters (run-id)")
	since := fs.String("since", "", "duration filter for --dlq (e.g. 2h)")
	where := fs.String("where", "", "CEL filter over {payload, meta} for --dlq (e.g. 'meta.region == \"eu\"')")
	from := fs.Int("from", -1, "spool sequence to replay from (--spool)")
	to := fs.Int("to", -1, "spool sequence to replay through, inclusive (--spool)")
	at := fs.String("at", "", "target node for re-injection (default: each message's origin node)")
	dryRun := fs.Bool("dry-run", false, "explain the paths instead of delivering")
	ids := fs.String("ids", "", "comma-separated dead letter ids to replay (--dlq)")
	limit := fs.Int("limit", 100, "maximum messages to replay")
	dataDir := fs.String("data-dir", "data", "SQLite storage directory")
	ephemeral := fs.Bool("ephemeral", false, "run the replay engine against an in-memory store (sinks still real)")
	del := fs.Bool("delete", false, "delete replayed dead letters after successful reinjection")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "replay: --config is required")
		return 2
	}
	modes := 0
	for _, m := range []bool{*dlq, *spoolMode, *jobRun != ""} {
		if m {
			modes++
		}
	}
	if modes != 1 {
		fmt.Fprintln(os.Stderr, "replay: choose exactly one mode: --dlq | --spool | --job <run-id>")
		return 2
	}
	if *spoolMode && *from < 0 {
		fmt.Fprintln(os.Stderr, "replay: --spool requires --from <spool-seq>")
		return 2
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 2
	}
	lr := config.LoadFile(*configPath)
	if lr.HasErrors() {
		printDiagsStderr(lr.Diagnostics)
		return 1
	}
	if lr.Pipeline.IsJob() {
		fmt.Fprintln(os.Stderr, "replay: replaying into a job pipeline re-runs its transforms; continuous-style reinjection is intended (job runs replay via --job)")
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		printDiagsStderr(diags)
		return 1
	}

	// Collect the messages to replay.
	type item struct {
		id    int64 // dead letter id (0 for spool rows)
		node  string
		msgID string
		raw   []byte
		meta  map[string]any
	}
	var items []item
	var st store.Store
	var dlIDs []int64

	if *ephemeral {
		st = store.NewMemory(pip.Config.Name)
	} else {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "replay: data dir: %v\n", err)
			return 2
		}
		sqlite, err := store.OpenSQLite(filepath.Join(*dataDir, "eventboat.db"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: open store: %v\n", err)
			return 2
		}
		st = sqlite
	}
	defer func() { _ = st.Close() }()

	switch {
	case *dlq || *jobRun != "":
		var dls []store.DeadLetter
		if *jobRun != "" {
			dls, err = st.DeadLettersForRun(pip.Config.Name, *jobRun)
		} else {
			sinceT := time.Time{}
			if *since != "" {
				d, err := config.ParseDuration(*since)
				if err != nil {
					fmt.Fprintf(os.Stderr, "replay: --since %q: %v\n", *since, err)
					return 2
				}
				sinceT = time.Now().Add(-d)
			}
			dls, err = st.DeadLettersSince(pip.Config.Name, sinceT)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: %v\n", err)
			return 2
		}
		idFilter := map[int64]bool{}
		if *ids != "" {
			for _, part := range strings.Split(*ids, ",") {
				if id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64); err == nil {
					idFilter[id] = true
					dlIDs = append(dlIDs, id)
				}
			}
		}
		var wherePred *celhost.Predicate
		if *where != "" {
			env, err := celhost.NewEnv(pip.Constants, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "replay: %v\n", err)
				return 2
			}
			wherePred, err = env.Compile(*where)
			if err != nil {
				fmt.Fprintf(os.Stderr, "replay: --where %q: %v\n", *where, err)
				return 2
			}
		}
		for _, dl := range dls {
			if len(items) >= *limit {
				break
			}
			if len(idFilter) > 0 && !idFilter[dl.ID] {
				continue
			}
			if wherePred != nil {
				var payload any
				_ = json.Unmarshal(dl.Raw, &payload)
				ok, evalErr := wherePred.Eval(payload, dl.Meta)
				if evalErr != nil || !ok {
					continue
				}
			}
			node := dl.Node
			if *at != "" {
				node = *at
			}
			items = append(items, item{id: dl.ID, node: node, msgID: dl.MessageID, raw: dl.Raw, meta: dl.Meta})
			if dl.ID > 0 {
				dlIDs = append(dlIDs, dl.ID) // eligible for --delete
			}
		}
	case *spoolMode:
		last := int64(*from) - 1
		for {
			var batch []item
			l, more, err := st.ReplayPage(pip.Config.Name, last, 200, func(seq int64, msg registry.Message, _ time.Time) error {
				if *to > 0 && seq > int64(*to) {
					return errStopPaging
				}
				if len(items)+len(batch) >= *limit {
					return errStopPaging
				}
				node := firstNonEmptyStr(msg.Meta["source"], msg.SrcName)
				if *at != "" {
					node = *at
				}
				batch = append(batch, item{node: node, msgID: msg.ID, raw: msg.Raw, meta: msg.Meta})
				return nil
			})
			_ = l
			items = append(items, batch...)
			if err != nil && err != errStopPaging {
				fmt.Fprintf(os.Stderr, "replay: spool walk: %v\n", err)
				return 2
			}
			if err == errStopPaging || !more {
				break
			}
			last = l
		}
	}

	if len(items) == 0 {
		if jsonOut {
			fmt.Println(`{"replayed": 0}`)
		} else {
			fmt.Println("replay: nothing matched")
		}
		return 0
	}

	// --dry-run: explain each message's predicted path; no engine, no sinks.
	if *dryRun {
		for _, it := range items {
			fmt.Printf("--- %s (node %s)\n", it.msgID, it.node)
			trace, err := explain.Trace(pip, explain.Options{Message: it.raw, EntryNode: entryNodeFor(pip, it.node)})
			if err != nil {
				fmt.Printf("  explain error: %v\n", err)
				continue
			}
			for _, line := range strings.Split(strings.TrimRight(trace, "\n"), "\n") {
				fmt.Println("  " + line)
			}
		}
		if jsonOut {
			b, _ := json.Marshal(map[string]any{"dry_run": true, "would_replay": len(items)})
			fmt.Println(string(b))
		}
		return 0
	}

	// Live replay: run the engine with REAL sinks, inject, wait for settle.
	eng, err := engine.New(pip, st, reg, engine.DefaultOptions().WithLimits(pip.Config.Limits))
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- eng.Run(ctx) }()
	for i := 0; i < 500 && !eng.Ready(); i++ {
		time.Sleep(2 * time.Millisecond)
	}

	replayed := 0
	failed := 0
	for _, it := range items {
		if _, err := eng.InjectReplay(it.node, it.raw, it.meta, it.msgID); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "replay: inject %s at %s: %v\n", it.msgID, it.node, err)
			continue
		}
		replayed++
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	werr := eng.WaitSettled(waitCtx)
	waitCancel()
	eng.Close()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
	}

	if *del && len(dlIDs) > 0 && failed == 0 {
		if n, err := st.DeleteDeadLetters(pip.Config.Name, dlIDs); err != nil {
			fmt.Fprintf(os.Stderr, "replay: delete dead letters: %v\n", err)
		} else {
			_ = n
		}
	}

	if jsonOut {
		b, _ := json.Marshal(map[string]any{"replayed": replayed, "failed": failed, "settled_err": errString(werr)})
		fmt.Println(string(b))
	} else {
		fmt.Printf("replay: reinjected %d message(s) at node(s) %q; settle: %v\n", replayed, *at, werr)
	}
	if failed > 0 {
		return 1
	}
	return 0
}

// entryNodeFor maps an internal injection target to the explain entry: for
// sinks and transforms the trace starts at their upstream source (symbolic
// best-effort; --at already narrows real reinjection).
func entryNodeFor(pip *ir.Pipeline, node string) string {
	if n, ok := pip.Nodes[node]; ok && n.Section == config.SectionSource {
		return node
	}
	for _, name := range pip.Order {
		if pip.Nodes[name].Section == config.SectionSource {
			return name
		}
	}
	return node
}

var errStopPaging = fmt.Errorf("stop paging")

func firstNonEmptyStr(vals ...any) string {
	for _, v := range vals {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
