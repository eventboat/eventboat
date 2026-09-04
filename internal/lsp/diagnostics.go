package lsp

import (
	"strings"

	"github.com/eventboat/eventboat/internal/config"
)

// LSP protocol shape subset used for diagnostics.

type publishDiagnosticsParams struct {
	URI         string        `json:"uri"`
	Diagnostics []lspDiag     `json:"diagnostics"`
}

type lspDiag struct {
	Range    lspRange `json:"range"`
	Severity int      `json:"severity"` // 1 error, 2 warning
	Code     string   `json:"code,omitempty"`
	Source   string   `json:"source,omitempty"`
	Message  string   `json:"message"`
}

type lspRange struct {
	Start lspPosition `json:"start"`
	End   lspPosition `json:"end"`
}

type lspPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// toLspDiagnostics maps engine diagnostics onto LSP ranges. Engine lines
// are 1-based; LSP positions are 0-based. Column information does not exist
// engine-side, so a diagnostic spans the whole line.
func toLspDiagnostics(text string, diags []config.Diagnostic) []lspDiag {
	lines := strings.Split(text, "\n")
	out := make([]lspDiag, 0, len(diags))
	for _, d := range diags {
		line := d.Line - 1
		if line < 0 {
			line = 0
		}
		if line >= len(lines) {
			line = len(lines) - 1
			if line < 0 {
				line = 0
			}
		}
		severity := 2
		if d.Severity == "error" {
			severity = 1
		}
		msg := d.Message
		if d.Hint != "" {
			msg += "\n" + d.Hint
		}
		out = append(out, lspDiag{
			Range: lspRange{
				Start: lspPosition{Line: line, Character: 0},
				End:   lspPosition{Line: line, Character: len(lines[line])},
			},
			Severity: severity,
			Code:     d.Code,
			Source:   "eventboat",
			Message:  msg,
		})
	}
	return out
}
