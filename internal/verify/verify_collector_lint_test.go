package verify

import "testing"

// The collection lints escalate under --strict through the same generic
// StrictOK path as every other warning (design §2.6.3).
func TestCollectorLintStrictEscalation(t *testing.T) {
	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: collector-lint-strict }
sources:
  logs:
    decoder: json
    file: { path: logs/*.jsonl }
sinks:
  out:
    depends_on: [logs]
    encoder: json
    file: { path: out.jsonl }
`
	reg := testRegistry(t)
	loose := Bytes("p.yaml", []byte(pipeline), "", reg, Options{})
	if !loose.OK || !loose.Diagnostics.HasWarnings() {
		t.Fatalf("non-strict verdict: OK=%v diags=%+v", loose.OK, loose.Diagnostics)
	}
	found := false
	for _, d := range loose.Diagnostics {
		if d.Code == "lint_collector_batch_one" && d.Severity == "warning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected lint_collector_batch_one as a warning, got %+v", loose.Diagnostics)
	}

	strict := Bytes("p.yaml", []byte(pipeline), "", reg, Options{Strict: true})
	if strict.OK {
		t.Fatalf("strict verdict passed with lint warnings: %+v", strict.Diagnostics)
	}
}
