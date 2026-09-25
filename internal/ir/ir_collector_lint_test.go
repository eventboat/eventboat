package ir

import (
	"testing"

	"github.com/eventboat/eventboat/internal/config"
)

// The three log-collection guardrails (design §2.6.3): each has a trigger and
// a no-trigger case, all warning-severity (--strict escalates through the one
// generic Diagnostics.StrictOK path).

func fileSourcePipeline(sourceBlock, sinkBlock string) string {
	return `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  logs:
    decoder: json
    file: ` + sourceBlock + `
sinks:
  out:
    depends_on: [logs]
    ` + sinkBlock + `
`
}

func hasWarning(diags []config.Diagnostic, code string) bool {
	for _, d := range diags {
		if d.Code == code {
			return d.Severity == "warning"
		}
	}
	return false
}

func TestLintLineBytesOverVL(t *testing.T) {
	// Trigger: max_line_bytes above VL's default, with a victorialogs sink.
	_, diags := build(t, fileSourcePipeline(
		`{ path: a.jsonl, max_line_bytes: 1048576 }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }`))
	if !hasWarning(diags, "lint_line_bytes_over_vl") {
		t.Fatalf("expected lint_line_bytes_over_vl, got %+v", diags)
	}
	// No trigger: the same bound without a victorialogs sink.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl, max_line_bytes: 1048576 }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    file: { path: out.jsonl }`))
	if hasCode(diags, "lint_line_bytes_over_vl") {
		t.Fatalf("lint fired without a victorialogs sink: %+v", diags)
	}
	// No trigger: the aligned bound (the default) with a victorialogs sink.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl, max_line_bytes: 262144 }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }`))
	if hasCode(diags, "lint_line_bytes_over_vl") {
		t.Fatalf("lint fired at the VL-aligned bound: %+v", diags)
	}
}

func TestLintMultilineNoTimeout(t *testing.T) {
	// Trigger: an explicit timeout_ms: 0.
	_, diags := build(t, fileSourcePipeline(
		`{ path: a.jsonl, multiline: { pattern: "^\\s", timeout_ms: 0 } }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    file: { path: out.jsonl }`))
	if !hasWarning(diags, "lint_multiline_no_timeout") {
		t.Fatalf("expected lint_multiline_no_timeout, got %+v", diags)
	}
	// No trigger: timeout_ms omitted (the 2s default is not a problem).
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl, multiline: { pattern: "^\\s" } }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    file: { path: out.jsonl }`))
	if hasCode(diags, "lint_multiline_no_timeout") {
		t.Fatalf("lint fired on the default timeout: %+v", diags)
	}
	// No trigger: a positive timeout.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl, multiline: { pattern: "^\\s", timeout_ms: 100 } }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    file: { path: out.jsonl }`))
	if hasCode(diags, "lint_multiline_no_timeout") {
		t.Fatalf("lint fired on a positive timeout: %+v", diags)
	}
}

func TestLintCollectorBatchOne(t *testing.T) {
	// Trigger: a file source plus a real sink without a batch block.
	_, diags := build(t, fileSourcePipeline(
		`{ path: a.jsonl }`,
		`encoder: json
    file: { path: out.jsonl }`))
	if !hasWarning(diags, "lint_collector_batch_one") {
		t.Fatalf("expected lint_collector_batch_one, got %+v", diags)
	}
	// Trigger: an explicit size: 1.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl }`,
		`encoder: json
    batch: { size: 1, timeout_ms: 1000 }
    file: { path: out.jsonl }`))
	if !hasWarning(diags, "lint_collector_batch_one") {
		t.Fatalf("expected lint_collector_batch_one for size: 1, got %+v", diags)
	}
	// No trigger: a batch size above 1.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl }`,
		`encoder: json
    batch: { size: 100, timeout_ms: 1000 }
    file: { path: out.jsonl }`))
	if hasCode(diags, "lint_collector_batch_one") {
		t.Fatalf("lint fired on batch.size 100: %+v", diags)
	}
	// No trigger: drop/debug sinks are not collection destinations.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl }`,
		`debug: {}`))
	if hasCode(diags, "lint_collector_batch_one") {
		t.Fatalf("lint fired on a debug sink: %+v", diags)
	}
	// No trigger: no file source (a cron source with an unbacthed sink is a
	// different shape).
	_, diags = build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  tick:
    cron: { expression: "* * * * *" }
sinks:
  out:
    depends_on: [tick]
    encoder: json
    file: { path: out.jsonl }
`)
	if hasCode(diags, "lint_collector_batch_one") {
		t.Fatalf("lint fired without a file source: %+v", diags)
	}
}

// An invalid multiline pattern fails construction (verify error), not silently
// disables aggregation.
func TestMultilineInvalidPatternIsVerifyError(t *testing.T) {
	_, diags := build(t, fileSourcePipeline(
		`{ path: a.jsonl, multiline: { pattern: "(" } }`,
		`encoder: json
    file: { path: out.jsonl }`))
	if !hasCode(diags, "plugin_schema") {
		t.Fatalf("expected plugin_schema for the bad pattern, got %+v", diags)
	}
}
