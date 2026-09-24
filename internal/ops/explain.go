package ops

import (
	"fmt"

	"github.com/eventboat/eventboat/internal/explain"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/verify"
)

// ExplainRequest selects the walkthrough mode. It is one shape on every
// surface (CLI --message/--at/--topology, MCP/Admin explain), so --at means
// the same thing everywhere.
type ExplainRequest struct {
	Message   []byte // sample message (nil = symbolic mode)
	EntryNode string // message-mode entry node ("" = first source, declaration order)
	Topology  bool   // render mermaid + ASCII instead of a trace
}

// ExplainResult carries every rendering the entry produces; hosts pick the
// fields they print (the CLI's --json topology shape needs both halves).
type ExplainResult struct {
	Trace   string // symbolic or message-level trace (topology mode: empty)
	Mermaid string
	ASCII   string
}

// Text renders the human-readable form: the trace, or mermaid + ASCII.
func (r *ExplainResult) Text() string {
	if r.Mermaid != "" || r.ASCII != "" {
		return r.Mermaid + "\n\n" + r.ASCII
	}
	return r.Trace
}

// ExplainPipeline renders one already-verified pipeline — the single
// Service-free explain entry (candidate 05). Rendering semantics stay in
// internal/explain (candidate 09 owns them); this function owns only the
// entry-node/topology shape shared by CLI, MCP and Admin.
func ExplainPipeline(pip *ir.Pipeline, req ExplainRequest) (*ExplainResult, error) {
	if req.Topology {
		return &ExplainResult{Mermaid: explain.TopologyMermaid(pip), ASCII: explain.TopologyASCII(pip)}, nil
	}
	opts := explain.Options{EntryNode: req.EntryNode}
	if len(req.Message) > 0 {
		opts.Message = req.Message
	}
	trace, err := explain.Trace(pip, opts)
	if err != nil && trace == "" {
		return nil, err
	}
	return &ExplainResult{Trace: trace}, nil
}

// ExplainContent is the content-based entry used by MCP/Admin: verify first
// (the one composition), then render. A failing verify travels as the
// explain error — the same reason text every surface reports.
func ExplainContent(reg *registry.Registry, content, baseDir string, req ExplainRequest) (string, error) {
	res := verify.Bytes("submitted.yaml", []byte(content), baseDir, reg, verify.Options{})
	if res.Pipeline == nil {
		return "", fmt.Errorf("explain: config errors: %s", res.Diagnostics.FirstErrorText())
	}
	out, err := ExplainPipeline(res.Pipeline, req)
	if err != nil {
		return "", err
	}
	return out.Text(), nil
}
