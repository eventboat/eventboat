// Package mcpserver exposes the ops service as MCP tools via the official
// Go SDK (github.com/modelcontextprotocol/go-sdk, v1.x stable — M2 review
// §一). Tool names follow the CLI concept names; dlq_query/dlq_replay keep
// the spec §3.4 names (the dlq abbreviation is retained industry-wide per
// spec v1.6; they were briefly dead_letter_* in the first M2 cut).
package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ops"
)

// NewServer builds the MCP server with all §3.4 tools registered.
func NewServer(svc *ops.Service, name, version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, nil)

	// --- read tools ---
	mcp.AddTool(server, &mcp.Tool{Name: "catalog",
		Description: "List registered plugins (sources/sinks/codecs) with their JSON Schemas — the basis for generating valid pipeline configs."},
		func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			c := svc.Catalog()
			return textResult(c), map[string]any{"sources": len(c.Sources), "sinks": len(c.Sinks), "codecs": len(c.Codecs)}, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "verify",
		Description: "Statically validate a pipeline configuration (YAML text): schema, topology, CEL+Starlark compilation, job rules. Returns structured diagnostics."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Config string `json:"config" jsonschema:"the full pipeline YAML text"`
		}) (*mcp.CallToolResult, any, error) {
			diags := svc.Verify(in.Config)
			ok := true
			for _, d := range diags {
				if d.Severity == "error" {
					ok = false
				}
			}
			return textResult(map[string]any{"ok": ok, "diagnostics": diags}), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "test",
		Description: "Run a contract test suite (§3.2) against the real in-process engine and return the per-case results. Pass the pipeline YAML as text alongside the suite (no shared filesystem)."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Suite    string `json:"suite" jsonschema:"the contract suite YAML text (suite/cases; pipeline: pipeline.yaml)"`
			Pipeline string `json:"pipeline" jsonschema:"the pipeline YAML text the suite runs against"`
		}) (*mcp.CallToolResult, any, error) {
			report, err := svc.Test(in.Suite, in.Pipeline)
			if err != nil {
				return nil, nil, err
			}
			return textResult(report), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "explain",
		Description: "Deterministic walkthrough of a pipeline configuration: symbolic by default, message-level when a sample JSON is given (scripts dry-run, CEL edges evaluated). topology=true renders mermaid+ASCII."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Config   string `json:"config" jsonschema:"the full pipeline YAML text"`
			Message  string `json:"message,omitempty" jsonschema:"sample message JSON for message-level evaluation"`
			Topology bool   `json:"topology,omitempty" jsonschema:"render the DAG (mermaid + ASCII) instead of a trace"`
		}) (*mcp.CallToolResult, any, error) {
			out, err := svc.Explain(in.Config, in.Message, in.Topology)
			if err != nil {
				return nil, nil, err
			}
			return textResult(out), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "status",
		Description: "Snapshot of every deployed pipeline: nodes, in-flight, checkpoint, counters, message rate, recent job runs."},
		func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			return textResult(svc.Status()), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "jobs",
		Description: "List job run history for a deployed job pipeline (run ids, status, parameters, counts)."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string `json:"pipeline"`
			Limit    int    `json:"limit,omitempty" jsonschema:"how many runs (default 20)"`
		}) (*mcp.CallToolResult, any, error) {
			runs, err := svc.Jobs(in.Pipeline, in.Limit)
			if err != nil {
				return nil, nil, err
			}
			return textResult(runs), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "tail",
		Description: "The most recent sampled deliveries at one node (bounded, payloads truncated to 512 bytes)."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Node string `json:"node"`
			N    int    `json:"n,omitempty" jsonschema:"how many entries (default 20)"`
		}) (*mcp.CallToolResult, any, error) {
			return textResult(svc.Tail(in.Node, in.N)), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "dlq_query",
		Description: "Query the dead letter queue of a deployed pipeline: since duration, CEL where-filter over {payload, meta}, limit."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string `json:"pipeline"`
			Since    string `json:"since,omitempty" jsonschema:"duration window, e.g. 2h"`
			Where    string `json:"where,omitempty" jsonschema:"CEL over {payload, meta}, e.g. 'meta.region == \"eu\"'"`
			Limit    int    `json:"limit,omitempty"`
		}) (*mcp.CallToolResult, any, error) {
			dls, err := svc.DeadLetterQuery(in.Pipeline, in.Since, in.Where, in.Limit)
			if err != nil {
				return nil, nil, err
			}
			return textResult(dls), any(nil), nil
		})

	// --- write tools (all verify-first) ---

	mcp.AddTool(server, &mcp.Tool{Name: "deploy",
		Description: "Deploy a pipeline configuration (YAML text): verify first — a failing verify rejects the deploy with diagnostics — then drain any previous instance and start the new one."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Config string `json:"config" jsonschema:"the full pipeline YAML text"`
		}) (*mcp.CallToolResult, any, error) {
			summary, err := svc.Deploy(ctx, in.Config)
			if err != nil {
				return nil, nil, err
			}
			return textResult(summary), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "trigger",
		Description: "Manually fire a job pipeline run, optionally with parameters (backfill ranges). wait=true blocks until the run reaches a terminal state and returns it."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline   string         `json:"pipeline"`
			Parameters map[string]any `json:"parameters,omitempty"`
			Wait       bool           `json:"wait,omitempty" jsonschema:"block until the run finishes (default false)"`
		}) (*mcp.CallToolResult, any, error) {
			jr, err := svc.Trigger(ctx, in.Pipeline, in.Parameters, in.Wait)
			if err != nil {
				return nil, nil, err
			}
			return textResult(jr), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "dlq_replay",
		Description: "Re-inject selected dead letters of a deployed continuous pipeline at a node (default: each letter's origin node). Replays keep their original message_id and are stamped is_replay=true."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string  `json:"pipeline"`
			Ids      []int64 `json:"ids,omitempty" jsonschema:"dead letter ids (default: all)"`
			At       string  `json:"at,omitempty" jsonschema:"target node"`
		}) (*mcp.CallToolResult, any, error) {
			n, err := svc.DeadLetterReplay(in.Pipeline, in.Ids, in.At)
			if err != nil {
				return nil, nil, err
			}
			return textResult(map[string]any{"replayed": n}), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "drain",
		Description: "Stop a deployed pipeline's sources and wait for in-flight work to commit. The pipeline stays deployed (drained)."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string `json:"pipeline"`
		}) (*mcp.CallToolResult, any, error) {
			if err := svc.Drain(in.Pipeline); err != nil {
				return nil, nil, err
			}
			return textResult(map[string]any{"pipeline": in.Pipeline, "status": "drained"}), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "pause",
		Description: "Pause a pipeline: stop source pulls (sources resume from their persisted states on resume; at-least-once covers the window)."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string `json:"pipeline"`
		}) (*mcp.CallToolResult, any, error) {
			if err := svc.Pause(in.Pipeline); err != nil {
				return nil, nil, err
			}
			return textResult(map[string]any{"pipeline": in.Pipeline, "status": "paused"}), any(nil), nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "resume",
		Description: "Resume a paused pipeline: sources restart from their persisted commit states."},
		func(ctx context.Context, req *mcp.CallToolRequest, in struct {
			Pipeline string `json:"pipeline"`
		}) (*mcp.CallToolResult, any, error) {
			if err := svc.Resume(ctx, in.Pipeline); err != nil {
				return nil, nil, err
			}
			return textResult(map[string]any{"pipeline": in.Pipeline, "status": "running"}), any(nil), nil
		})

	return server
}

// textResult marshals v as the tool's structured output (and as pretty JSON
// text for hosts that only render text).
func textResult(v any) *mcp.CallToolResult {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		b = []byte(fmt.Sprintf("%v", v))
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}
}

var _ = config.Diagnostic{} // keep the config import for doc symmetry
