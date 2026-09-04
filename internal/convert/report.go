package convert

import (
	"fmt"
	"strings"

	"github.com/eventboat/eventboat/internal/config"
)

// Report is the v2 → v3 migration report (§7.3: dual list — auto-migrated
// vs needs-human — plus per-item reasons and suggested rewrites).
type Report struct {
	Source      string
	Style       string
	Notes       []string
	StmtGroups  []StmtGroup
	Manuals     []manualItem
	VerifyOK    bool
	VerifyDiags []config.Diagnostic
}

// StmtGroup is the statement table of one dsl block.
type StmtGroup struct {
	Where string
	Rows  []stmtRow
}

// AutoStatements counts machine-verified auto conversions.
func (r *Report) AutoStatements() int {
	n := 0
	for _, g := range r.StmtGroups {
		for _, row := range g.Rows {
			if row.Status == "auto" {
				n++
			}
		}
	}
	return n
}

func (r *Report) ManualStatements() int {
	n := 0
	for _, g := range r.StmtGroups {
		for _, row := range g.Rows {
			if row.Status != "auto" {
				n++
			}
		}
	}
	return n
}

// Markdown renders the report.
func (r *Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# eventboat convert — v2 → v3 migration report\n\n")
	fmt.Fprintf(&b, "- **Source**: %s (style: %s)\n", r.Source, r.Style)
	fmt.Fprintf(&b, "- **Statements**: %d auto / %d manual\n", r.AutoStatements(), r.ManualStatements())
	fmt.Fprintf(&b, "- **Structural notes**: %d · **Manual items**: %d\n", len(r.Notes), len(r.Manuals))
	verdict := "PASS"
	if !r.VerifyOK {
		verdict = "FAIL"
	}
	errs, warns := 0, 0
	for _, d := range r.VerifyDiags {
		if d.Severity == "error" {
			errs++
		} else {
			warns++
		}
	}
	fmt.Fprintf(&b, "- **Verify (v3)**: %s (%d errors, %d warnings)\n", verdict, errs, warns)

	if len(r.Notes) > 0 {
		b.WriteString("\n## Structural transformations\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}

	for _, g := range r.StmtGroups {
		if len(g.Rows) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n## Statement conversions — %s\n\n", g.Where)
		b.WriteString("| v2 (eql) | v3 (starlark/cel) | status |\n|---|---|---|\n")
		for _, row := range g.Rows {
			src := strings.ReplaceAll(row.Source, "|", "\\|")
			res := strings.ReplaceAll(row.Result, "|", "\\|")
			res = strings.ReplaceAll(res, "\n", " ⏎ ")
			fmt.Fprintf(&b, "| `%s` | `%s` | %s |\n", src, res, row.Status)
		}
	}

	if len(r.Manuals) > 0 {
		b.WriteString("\n## Manual items\n")
		for i, m := range r.Manuals {
			fmt.Fprintf(&b, "\n### [M%d] %s\n\n", i+1, m.Where)
			fmt.Fprintf(&b, "- **Reason**: %s\n", m.Reason)
			fmt.Fprintf(&b, "- **Suggestion**: %s\n", m.Suggestion)
		}
	}

	if len(r.VerifyDiags) > 0 {
		b.WriteString("\n## Verify diagnostics (v3 gate on the generated file)\n\n")
		b.WriteString("```\n")
		for _, d := range r.VerifyDiags {
			fmt.Fprintf(&b, "%s\n", d.Error())
		}
		b.WriteString("```\n")
	}
	return b.String()
}
