package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/dlq"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/runtimecfg"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/verify"
)

// cmdExplain renders the deterministic pipeline walkthrough: symbolic by
// default, message-level with --message (CEL edges really evaluated,
// Starlark scripts really dry-run — the sandbox is deterministic), plus
// --topology for mermaid + ASCII renderings (§3.3). It delegates to the
// shared ops explain entry (candidate 05): --at behaves identically here and
// on MCP/Admin.
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
	res := verify.File(*configPath, reg, verify.Options{ForExplain: true})
	if res.Pipeline == nil {
		printDiagsStderr(res.Diagnostics)
		return 1
	}
	defer func() { _ = res.Pipeline.Close() }()

	req := ops.ExplainRequest{EntryNode: *entry, Topology: *topology}
	if *message != "" {
		raw, err := os.ReadFile(*message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "explain: read message: %v\n", err)
			return 2
		}
		req.Message = raw
	}
	out, err := ops.ExplainPipeline(res.Pipeline, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "explain: %v\n", err)
		return 2
	}
	if *topology {
		if jsonOut {
			b, _ := json.Marshal(map[string]string{"mermaid": out.Mermaid, "ascii": out.ASCII})
			fmt.Println(string(b))
		} else {
			fmt.Println(out.Mermaid)
			fmt.Println()
			fmt.Print(out.ASCII)
		}
		return 0
	}
	fmt.Print(out.Trace)
	return 0
}

// cmdReplay re-injects dead letters, spool windows or one job run's dead
// letters into a live pipeline (§3.3). --dry-run explains instead of
// delivering. Dead-letter selection (filter compilation with the pipeline's
// constants, ids/limit ordering, the codec-carrying request) is the shared
// dlq contract; this verb only chooses the transport (a local engine).
func cmdReplay(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	dlqMode := fs.Bool("dlq", false, "replay dead letters (filtered by --since/--where)")
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
	for _, m := range []bool{*dlqMode, *spoolMode, *jobRun != ""} {
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
	res := verify.File(*configPath, reg, verify.Options{ForExplain: true})
	if res.Pipeline == nil {
		printDiagsStderr(res.Diagnostics)
		return 1
	}
	pip := res.Pipeline
	defer func() { _ = pip.Close() }()
	if pip.Config.IsJob() {
		fmt.Fprintln(os.Stderr, "replay: replaying into a job pipeline re-runs its transforms; continuous-style reinjection is intended (job runs replay via --job)")
	}

	// Collect the messages to replay: one shared request shape for both
	// transports (candidate 05), carrying codec identity with the payload.
	var items []dlq.Request

	owner := newStoreOwner(runtimecfg.Storage{DataDir: *dataDir, Ephemeral: *ephemeral})
	defer func() { _ = owner.Close() }()
	st, err := owner.Open(pip.Config.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: open store: %v\n", err)
		return 2
	}

	switch {
	case *dlqMode || *jobRun != "":
		var dls []store.DeadLetter
		if *jobRun != "" {
			dls, err = st.DeadLettersForRun(pip.Config.Name, *jobRun)
		} else {
			sinceT, serr := dlq.Since(*since, time.Now())
			if serr != nil {
				fmt.Fprintf(os.Stderr, "replay: %v\n", serr)
				return 2
			}
			dls, err = st.DeadLettersSince(pip.Config.Name, sinceT)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: %v\n", err)
			return 2
		}
		items, err = dlq.Select(dls, dlq.Filter{
			Where: *where,
			IDs:   parseIDs(*ids),
			Limit: *limit,
			At:    *at,
		}, pip.Constants)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: %v\n", err)
			return 2
		}
	case *spoolMode:
		last := int64(*from) - 1
		for {
			var batch []dlq.Request
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
				batch = append(batch, dlq.Request{Node: node, MessageID: msg.ID, Codec: msg.Codec, Raw: msg.Raw, Meta: msg.Meta})
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
			fmt.Printf("--- %s (node %s)\n", it.MessageID, it.Node)
			out, err := ops.ExplainPipeline(pip, ops.ExplainRequest{Message: it.Raw, EntryNode: entryNodeFor(pip, it.Node)})
			if err != nil {
				fmt.Printf("  explain error: %v\n", err)
				continue
			}
			for _, line := range strings.Split(strings.TrimRight(out.Trace, "\n"), "\n") {
				fmt.Println("  " + line)
			}
		}
		if jsonOut {
			b, _ := json.Marshal(map[string]any{"dry_run": true, "would_replay": len(items)})
			fmt.Println(string(b))
		}
		return 0
	}

	// Live replay: run the engine with REAL sinks, inject, wait for commit.
	// A replay engine is a writer: take the cross-process store lease, or
	// refuse while a daemon owns the pipeline (candidate 08).
	lease, err := acquireRunLease(owner, "replay", pip.Config.Name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = lease.Release() }()
	eng, err := engine.New(pip, st, reg, engine.DefaultOptions().WithLimits(pip.Config.Limits))
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- eng.Run(ctx) }()
	for i := 0; i < 500 && !eng.Ready(); i++ {
		time.Sleep(2 * time.Millisecond)
	}

	replayed := 0
	failed := 0
	for _, it := range items {
		if _, err := eng.InjectReplay(it.Node, it.Message()); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "replay: inject %s at %s: %v\n", it.MessageID, it.Node, err)
			continue
		}
		replayed++
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	werr := eng.WaitCommit(waitCtx)
	waitCancel()
	eng.Close()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
	}

	// Delete only what was actually selected and replayed: `--ids` with a
	// limit must not delete rows that were never replayed (candidate 05).
	if *del && failed == 0 {
		if ids := dlq.IDs(items); len(ids) > 0 {
			if _, err := st.DeleteDeadLetters(pip.Config.Name, ids); err != nil {
				fmt.Fprintf(os.Stderr, "replay: delete dead letters: %v\n", err)
			}
		}
	}

	if jsonOut {
		b, _ := json.Marshal(map[string]any{"replayed": replayed, "failed": failed, "committed_err": errString(werr)})
		fmt.Println(string(b))
	} else {
		fmt.Printf("replay: reinjected %d message(s) at node(s) %q; commit: %v\n", replayed, *at, werr)
	}
	if failed > 0 {
		return 1
	}
	return 0
}

// parseIDs parses the comma-separated --ids flag; malformed entries are
// ignored (the flag's documented shape is a list of integers).
func parseIDs(s string) []int64 {
	if s == "" {
		return nil
	}
	var out []int64
	for _, part := range strings.Split(s, ",") {
		if id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64); err == nil {
			out = append(out, id)
		}
	}
	return out
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
