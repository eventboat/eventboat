// Package explain renders deterministic pipeline walkthroughs from the
// static IR (redesign-v3.md §3.3): symbolic traces without a message, and
// message-level traces with one — CEL edges are really evaluated and
// Starlark scripts really dry-run (the sandbox is deterministic and
// side-effect free, M2 review R10), so the answer matches what production
// would do with the same input.
package explain

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
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
	if err := json.Unmarshal(opts.Message, &decoded); err != nil {
		return "", fmt.Errorf("explain: sample message is not JSON: %w", err)
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

// walk visits one node's out-edges (message mode): scripts dry-run first so
// downstream conditions see the transformed payload.
func walk(pip *ir.Pipeline, node *ir.Node, payload any, meta map[string]any, b *strings.Builder) {
	if node.Script != nil {
		ps := starhost.NewMsgState("payload", payload)
		ms := starhost.NewMsgState("meta", meta)
		serr := node.Script.RunWithParams(ps, ms, pip.FrozenConstants, pip.FrozenParameters)
		if serr != nil {
			fmt.Fprintf(b, "%s: transform.script (starlark, %d statements, budget=%d steps)\n",
				node.Name, countStatements(node.Script.Source()), pip.StarOptions.MaxSteps)
			fmt.Fprintf(b, "  ✗ script failed at line %d: %s\n", serr.Line, serr.Msg)
			fmt.Fprintf(b, "  %s\n", indent(serr.Backtrace))
			fmt.Fprintf(b, "  → the message would dead-letter on the incoming edge's delivery policy\n")
			return
		}
		fmt.Fprintf(b, "%s: transform.script (starlark, %d statements, budget=%d steps) ✓\n",
			node.Name, countStatements(node.Script.Source()), pip.StarOptions.MaxSteps)
		if ps.Dirty() {
			payload = ps.GoValue()
		}
		if ms.Dirty() {
			if m, ok := ms.MapValue(); ok {
				meta = m
			}
		}
		} else if node.IsSplit {
			if arr, ok := payload.([]any); ok {
				fmt.Fprintf(b, "%s: transform.split → %d messages (children share the parent's identity)\n", node.Name, len(arr))
			} else {
				fmt.Fprintf(b, "%s: transform.split ✗ payload is not an array → dead letter\n", node.Name)
				return
			}
		} else if node.Wasm != nil {
			// The guest is not dry-run in explain: downstream conditions
			// evaluate against the pre-transform payload (documented).
			fmt.Fprintf(b, "%s: transform.wasm (module %s, entrypoint %s) — guest not dry-run; downstream sees the pre-transform payload\n",
				node.Name, node.Config.Wasm.Module, wasmEntry(node))
		}

	matched := 0
	for i := range node.Out {
		edge := &node.Out[i]
		mark := "✗ no match"
		if edge.When == nil {
			mark = "always"
			matched++
		} else {
		ok, evalErr := edge.When.Eval(payload, meta)
		switch {
		case evalErr != nil:
			mark = fmt.Sprintf("✗ evaluation error (counts as not-passed): %s", evalErr.Error())
		case ok:
			mark = "✓ MATCH"
			matched++
		}
		}
		fmt.Fprintf(b, "  %s → %s", node.Name, edge.To)
		if edge.WhenSource != "" {
			fmt.Fprintf(b, "  when %s", edge.WhenSource)
		} else {
			fmt.Fprintf(b, "            (no condition)")
		}
		fmt.Fprintf(b, "\t%s\n", mark)
	}
	if len(node.Out) > 0 && matched == 0 {
		fmt.Fprintf(b, "  (zero matching edges: the message settles as filtered — eventboat_fanout_no_match_total)\n")
	}
	// Recurse into matched downstream nodes (sinks described, not executed).
	for i := range node.Out {
		edge := &node.Out[i]
		if edge.When != nil {
			ok, evalErr := edge.When.Eval(payload, meta)
			if evalErr != nil || !ok {
				continue
			}
		}
		next := pip.Nodes[edge.To]
		switch next.Section {
		case config.SectionSink:
			describeSink(next, b)
		default:
			walk(pip, next, payload, meta, b)
		}
	}
}

func describeSink(node *ir.Node, b *strings.Builder) {
	detail := ""
	if node.Config.Batch != nil {
		detail = fmt.Sprintf(", batch=%d", node.Config.Batch.Size)
		if node.Config.Batch.TimeoutMs > 0 {
			detail += fmt.Sprintf("/%dms", node.Config.Batch.TimeoutMs)
		}
	}
	fmt.Fprintf(b, "  %s: sink %s (encoder %s%s)\n", node.Name, node.Config.Plugin, encoderOf(node), detail)
	fmt.Fprintf(b, "    delivery: retries=%d backoff=%s → settles on ack; exhausted → dead letter\n",
		sinkEdgeRetries(node), sinkEdgeBackoff(node))
}

func symbolic(pip *ir.Pipeline, b *strings.Builder) error {
	for _, name := range pip.Order {
		node := pip.Nodes[name]
		switch node.Section {
		case config.SectionSource:
			fmt.Fprintf(b, "%s: source %s (decoder %s)\n", name, node.Config.Plugin, decoderOf(node))
		case config.SectionTransform:
			if node.Script != nil {
				fmt.Fprintf(b, "%s: transform.script (starlark, %d statements, budget=%d steps)\n",
					name, countStatements(node.Script.Source()), pip.StarOptions.MaxSteps)
			} else if node.IsSplit {
				fmt.Fprintf(b, "%s: transform.split (array payload → one message per element)\n", name)
			} else if node.Wasm != nil {
				fmt.Fprintf(b, "%s: transform.wasm (module %s, entrypoint %s, budget %dms)\n",
					name, node.Config.Wasm.Module, wasmEntry(node), wasmBudget(node))
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
	fmt.Fprintf(b, "\nsettle: every branch reaching a sink settles on its ack (or dead letter after retries);\na message with zero matching edges settles as filtered.\n")
	return nil
}

func decoderOf(n *ir.Node) string {
	if n.Config.Decoder == "" {
		return "json"
	}
	return n.Config.Decoder
}

func wasmEntry(n *ir.Node) string {
	if n.Config.Wasm.Entrypoint != "" {
		return n.Config.Wasm.Entrypoint
	}
	return "transform"
}

func wasmBudget(n *ir.Node) int {
	if n.Config.Wasm.TimeoutMs > 0 {
		return n.Config.Wasm.TimeoutMs
	}
	return 1000
}

func encoderOf(n *ir.Node) string {
	if n.Config.Encoder == "" {
		return "json"
	}
	return n.Config.Encoder
}

func sinkEdgeRetries(node *ir.Node) int {
	for _, e := range node.In {
		return e.Retries
	}
	return 3
}

func sinkEdgeBackoff(node *ir.Node) string {
	for _, e := range node.In {
		return e.Backoff
	}
	return "exponential"
}

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
