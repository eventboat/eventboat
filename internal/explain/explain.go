// Package explain renders deterministic pipeline walkthroughs from the
// static IR (redesign-v3.md §3.3): symbolic traces without a message, and
// message-level traces with one — CEL edges are really evaluated and
// Starlark scripts really dry-run (the sandbox is deterministic and
// side-effect free, M2 review R10), so the answer matches what production
// would do with the same input.
package explain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

// Options tunes the walkthrough.
type Options struct {
	Message   []byte // sample message (nil = symbolic mode)
	EntryNode string // entry node for message mode ("" = first source)
}

// Trace renders the human-readable walkthrough.
func Trace(pip *ir.Pipeline, opts Options) (string, error) {
	var b strings.Builder
	if opts.Message == nil {
		fmt.Fprintf(&b, "pipeline %q — symbolic trace (no message; pass --message for message-level evaluation)\n\n", pip.Config.Name)
		if err := symbolic(pip, &b); err != nil {
			return "", err
		}
		return b.String(), nil
	}

	entry := opts.EntryNode
	if entry == "" {
		for _, name := range pip.Order {
			if pip.Nodes[name].Section == config.SectionSource {
				entry = name
				break
			}
		}
	}
	if entry == "" {
		return "", fmt.Errorf("explain: pipeline has no source node")
	}
	node, ok := pip.Nodes[entry]
	if !ok {
		return "", fmt.Errorf("explain: unknown entry node %q", entry)
	}

	var decoded any
	// Decode the sample with the source's declared decoder — the loader
	// materialized the default (candidate 06) and the build resolved the
	// instance the engine would use, so the header's `decoder %s` claim is
	// true and a csv/raw/declared source is not silently fed JSON.
	codec, ok := pip.Codec(node.Config.Decoder)
	if !ok {
		return "", fmt.Errorf("explain: decoder %q of source %q is not resolvable", node.Config.Decoder, entry)
	}
	decoded, err := codec.Decode(opts.Message)
	if err != nil {
		return "", fmt.Errorf("explain: sample message is not %s: %w", node.Config.Decoder, err)
	}
	meta := map[string]any{
		"message_id":   "explain",
		"ingest_time":  "explain",
		"source":       entry,
		"explain_mode": true,
	}
	if pip.Config.IsJob() {
		meta["job_run_id"] = "explain"
	}

	fmt.Fprintf(&b, "message enters at node %q (source %s / decoder %s)\n\n",
		entry, node.Config.Plugin, decoderOf(node))

	payload := decoded
	walk(pip, node, payload, meta, &b)
	return b.String(), nil
}

// walk visits one node's out-edges (message mode): explain-safe transforms
// dry-run first so downstream conditions see the transformed payload; other
// transforms (wasm: guest code must not execute in explain) pass the payload
// through unchanged and say so — the disclosure keeps downstream MATCH/
// no-match output honest about evaluating the pre-transform payload
// (documented, docs/wasm.md).
func walk(pip *ir.Pipeline, node *ir.Node, payload any, meta map[string]any, b *strings.Builder) {
	if node.Transform != nil {
		msg := &registry.Message{Decoded: payload, Meta: meta}
		outs, aerr := node.Transform.Apply(msg)
		if aerr != nil {
			fmt.Fprintf(b, "%s: %s\n", node.Name, transformLabel(node))
			fmt.Fprintf(b, "  ✗ %s\n", aerr.Error())
			var te *registry.TransformError
			if errors.As(aerr, &te) && te.Backtrace != "" {
				fmt.Fprintf(b, "  %s\n", indent(te.Backtrace))
			}
			fmt.Fprintf(b, "  → the message would dead-letter on the incoming edge's delivery policy\n")
			return
		}
		if len(outs) == 0 {
			fmt.Fprintf(b, "%s: %s → 0 outputs: the message commits as filtered\n", node.Name, transformLabel(node))
			return
		}
		if len(outs) > 1 {
			fmt.Fprintf(b, "%s: %s → %d messages (children share the parent's identity; walking child #1)\n",
				node.Name, transformLabel(node), len(outs))
		} else {
			fmt.Fprintf(b, "%s: %s ✓\n", node.Name, transformLabel(node))
		}
		payload, meta = outs[0].Decoded, outs[0].Meta
	} else if node.Section == config.SectionTransform {
		// Not dry-run (wasm — explain must not execute guest code; any
		// third-party plugin that skips the capability; or an explain-safe
		// instance a verify-only build closed): say so instead of silently
		// evaluating downstream edges, which would read as post-transform
		// output. The payload passes through unchanged (documented,
		// docs/wasm.md).
		switch {
		case node.Config.Plugin == "wasm":
			fmt.Fprintf(b, "%s: transform.wasm (module %s, entrypoint %s) — guest not dry-run; downstream sees the pre-transform payload\n",
				node.Name, wasmModule(node), wasmEntry(node))
		case node.ExplainSafe:
			fmt.Fprintf(b, "%s: %s — explain-safe instance not retained (verify-only build); downstream sees the pre-transform payload\n",
				node.Name, transformLabel(node))
		default:
			fmt.Fprintf(b, "%s: %s — plugin not dry-run (not explain-safe); downstream sees the pre-transform payload\n",
				node.Name, transformLabel(node))
		}
	}

	matched := 0
	// Each edge's predicate is evaluated exactly once per walk; the recursion
	// below reuses these results (candidate 09: it used to re-evaluate).
	evals := make([]edgeEval, 0, len(node.Out))
	for i := range node.Out {
		edge := &node.Out[i]
		passed := edge.When == nil
		mark := "always"
		if edge.When != nil {
			ok, evalErr := edge.When.Eval(payload, meta)
			switch {
			case evalErr != nil:
				mark = fmt.Sprintf("✗ evaluation error (counts as not-passed): %s", evalErr.Error())
			case ok:
				mark = "✓ MATCH"
			default:
				mark = "✗ no match"
			}
			passed = evalErr == nil && ok
		}
		if passed {
			matched++
		}
		evals = append(evals, edgeEval{edge: edge, passed: passed})
		fmt.Fprintf(b, "  %s → %s", node.Name, edge.To)
		if edge.WhenSource != "" {
			fmt.Fprintf(b, "  when %s", edge.WhenSource)
		} else {
			fmt.Fprintf(b, "            (no condition)")
		}
		fmt.Fprintf(b, "\t%s\n", mark)
	}
	if len(node.Out) > 0 && matched == 0 {
		fmt.Fprintf(b, "  (zero matching edges: the message commits as filtered — eventboat_fanout_no_match_total)\n")
	}
	// Recurse into matched downstream nodes (sinks described, not executed).
	for _, ev := range evals {
		if !ev.passed {
			continue
		}
		next := pip.Nodes[ev.edge.To]
		switch next.Section {
		case config.SectionSink:
			describeSink(next, b)
		default:
			walk(pip, next, payload, meta, b)
		}
	}
}

// edgeEval is one edge's cached predicate verdict for the current payload.
type edgeEval struct {
	edge   *ir.Edge
	passed bool
}

// describeSink renders one sink's resolved delivery semantics (candidate 09):
// every inbound edge's policy (never just the first one), the engine's batch
// aggregation rule for mixed batches, and the terminal consequence — a dead
// letter for required edges, a drop for required: false edges.
func describeSink(node *ir.Node, b *strings.Builder) {
	detail := ""
	if node.Config.Batch != nil {
		detail = fmt.Sprintf(", batch=%d", node.Config.Batch.Size)
		if node.Config.Batch.TimeoutMs > 0 {
			detail += fmt.Sprintf("/%dms", node.Config.Batch.TimeoutMs)
		}
	}
	fmt.Fprintf(b, "  %s: sink %s (encoder %s%s)\n", node.Name, node.Config.Plugin, encoderOf(node), detail)
	fmt.Fprintf(b, "    delivery: commits on ack; per in-edge policy:\n")
	for i := range node.In {
		e := &node.In[i]
		when := ""
		if e.WhenSource != "" {
			when = fmt.Sprintf(" (when %s)", e.WhenSource)
		}
		terminal := "exhausted → dead letter"
		if !e.Required {
			terminal = "exhausted → dropped (required: false; the engine drops and commits)"
		}
		fmt.Fprintf(b, "      %s%s: retries=%d backoff=%s timeout=%s required=%t → %s\n",
			e.From, when, e.Retries, e.Backoff, timeoutLabel(e.TimeoutMs), e.Required, terminal)
	}
	fmt.Fprintf(b, "    batch rule: a mixed batch takes the strictest retries and explicit timeout (max of each; the default timeout applies when no edge in the batch sets one)\n")
}

// timeoutLabel renders one edge's per-write timeout; 0 means the engine's
// runtime default (Options.DefaultTimeout), not a number explain may invent.
func timeoutLabel(ms int) string {
	if ms > 0 {
		return fmt.Sprintf("%dms", ms)
	}
	return "default"
}

func symbolic(pip *ir.Pipeline, b *strings.Builder) error {
	for _, name := range pip.Order {
		node := pip.Nodes[name]
		switch node.Section {
		case config.SectionSource:
			fmt.Fprintf(b, "%s: source %s (decoder %s)\n", name, node.Config.Plugin, decoderOf(node))
		case config.SectionTransform:
			switch {
			case node.Config.Plugin == "script":
				fmt.Fprintf(b, "%s: transform.script (starlark, %d statements, budget=%d steps)\n",
					name, countStatements(rawScript(node)), pip.StarOptions.MaxSteps)
			case node.Config.Plugin == "split":
				fmt.Fprintf(b, "%s: transform.split (array payload → one message per element)\n", name)
			case node.Config.Plugin == "wasm":
				fmt.Fprintf(b, "%s: transform.wasm (module %s, entrypoint %s, %s)\n",
					name, wasmModule(node), wasmEntry(node), wasmModeLabel(node))
			default:
				fmt.Fprintf(b, "%s: transform.%s\n", name, node.Config.Plugin)
			}
		case config.SectionSink:
			describeSink(node, b)
		}
		for _, edge := range node.Out {
			fmt.Fprintf(b, "  %s → %s", name, edge.To)
			if edge.WhenSource != "" {
				fmt.Fprintf(b, "  when %s", edge.WhenSource)
			} else {
				fmt.Fprintf(b, "              (no condition)")
			}
			fmt.Fprintln(b)
		}
	}
	fmt.Fprintf(b, "\ncommit: every branch reaching a sink commits on its ack (or dead letter after retries);\na message with zero matching edges commits as filtered.\n")
	return nil
}

// decoderOf returns the source's decoder; the loader materialized the
// default (candidate 06), so the field is authoritative.
func decoderOf(n *ir.Node) string { return n.Config.Decoder }

// transformLabel renders the transform line prefix in message mode; the
// builtins keep their descriptive labels, third-party plugins name
// themselves.
func transformLabel(node *ir.Node) string {
	switch node.Config.Plugin {
	case "script":
		return "transform.script"
	case "split":
		return "transform.split"
	case "wasm":
		return "transform.wasm"
	default:
		return "transform." + node.Config.Plugin
	}
}

// rawScript returns the Starlark source of a script-plugin transform node.
func rawScript(n *ir.Node) string {
	if s, ok := n.Config.PluginConfig.(string); ok {
		return s
	}
	return ""
}

// wasmCfgMap returns the raw wasm plugin block (no defaults injected).
func wasmCfgMap(n *ir.Node) map[string]any {
	m, _ := n.Config.PluginConfig.(map[string]any)
	return m
}

func wasmModule(n *ir.Node) string {
	if s, ok := wasmCfgMap(n)["module"].(string); ok {
		return s
	}
	return "?"
}

func wasmEntry(n *ir.Node) string {
	if s, ok := wasmCfgMap(n)["entrypoint"].(string); ok && s != "" {
		return s
	}
	return "transform"
}

// wasmMode resolves the per-invoke execution mode of a wasm node exactly the
// way the plugin adapter does (wasmhost.ResolveMode): fast mode when
// timeout_ms is unset, the budget when it is set. Explain renders this
// resolution instead of re-deriving a number.
func wasmMode(n *ir.Node) wasmhost.Mode {
	var cfg wasmhost.Config
	if m := wasmCfgMap(n); m != nil {
		if raw, err := json.Marshal(m); err == nil {
			_ = json.Unmarshal(raw, &cfg)
		}
	}
	return wasmhost.ResolveMode(&cfg)
}

// wasmModeLabel renders the resolved mode for the symbolic trace.
func wasmModeLabel(n *ir.Node) string {
	mode := wasmMode(n)
	if mode.Fast() {
		return "fast mode (no per-invoke kill switch)"
	}
	return fmt.Sprintf("budget %dms", mode.Timeout/time.Millisecond)
}

// encoderOf returns the sink's encoder; materialized by the loader
// (candidate 06).
func encoderOf(n *ir.Node) string { return n.Config.Encoder }

func countStatements(src string) int {
	n := 0
	for _, line := range strings.Split(src, "\n") {
		if strings.TrimSpace(line) != "" && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			n++
		}
	}
	return n
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "    " + l
	}
	return strings.Join(lines, "\n")
}
