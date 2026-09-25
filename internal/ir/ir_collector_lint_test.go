package ir

import (
	"fmt"
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

// D4: the line-bytes lint must see a generic http sink pointed at
// /insert/jsonline (it receives the same 200-while-skipping), and must not
// fire for an http sink that is not the jsonline endpoint.
func TestLintLineBytesOverVLHTTPJsonlineSink(t *testing.T) {
	_, diags := build(t, fileSourcePipeline(
		`{ path: a.jsonl, max_line_bytes: 1048576 }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    http: { url: "http://127.0.0.1:9428/INSERT/JsonLine" }`))
	if !hasWarning(diags, "lint_line_bytes_over_vl") {
		t.Fatalf("expected lint_line_bytes_over_vl for an http insert/jsonline sink, got %+v", diags)
	}
	// No trigger: a generic http endpoint that is not jsonline ingest.
	_, diags = build(t, fileSourcePipeline(
		`{ path: a.jsonl, max_line_bytes: 1048576 }`,
		`encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    http: { url: "http://127.0.0.1:9428/api/v1/write" }`))
	if hasCode(diags, "lint_line_bytes_over_vl") {
		t.Fatalf("lint fired for a non-jsonline http sink: %+v", diags)
	}
}

// D4: a `raw` decoder encodes to a JSON STRING, which the jsonline API skips
// server-side; the lint must warn for every unwrapped path to a VL sink and
// stay quiet when a wrap_field transform (or a different decoder) makes the
// payload an object, or when the branch never reaches VL.
func TestLintVLNonObjectPayload(t *testing.T) {
	rawPipeline := func(decoder, sinkBlock string) string {
		return `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  logs:
    decoder: ` + decoder + `
    file: { path: a.log }
sinks:
  out:
    depends_on: [logs]
    ` + sinkBlock + `
`
	}
	// Trigger: raw decoder straight into the victorialogs sink.
	_, diags := build(t, rawPipeline("raw", `encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }`))
	if !hasWarning(diags, "lint_vl_nonobject_payload") {
		t.Fatalf("expected lint_vl_nonobject_payload, got %+v", diags)
	}
	// Trigger: raw decoder into a generic http sink at /insert/jsonline (the
	// same server-side skipping applies).
	_, diags = build(t, rawPipeline("raw", `encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    http: { url: "http://127.0.0.1:9428/insert/jsonline" }`))
	if !hasWarning(diags, "lint_vl_nonobject_payload") {
		t.Fatalf("expected lint_vl_nonobject_payload for an http jsonline sink, got %+v", diags)
	}
	// No trigger: json decoding produces objects.
	_, diags = build(t, rawPipeline("json", `encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }`))
	if hasCode(diags, "lint_vl_nonobject_payload") {
		t.Fatalf("lint fired for a json decoder: %+v", diags)
	}
	// No trigger: a fields transform with wrap_field is on the path.
	_, diags = build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  logs:
    decoder: raw
    file: { path: a.log }
transforms:
  wrap:
    depends_on: [logs]
    fields: { wrap_field: msg }
sinks:
  out:
    depends_on: [wrap]
    encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }
`)
	if hasCode(diags, "lint_vl_nonobject_payload") {
		t.Fatalf("lint fired with a wrap_field transform on the path: %+v", diags)
	}
	// No trigger: the raw source feeds a branch that never reaches VL.
	_, diags = build(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  logs:
    decoder: raw
    file: { path: a.log }
  tick:
    cron: { expression: "* * * * *" }
sinks:
  rawout:
    depends_on: [logs]
    encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    file: { path: raw.jsonl }
  vlout:
    depends_on: [tick]
    encoder: json
    batch: { size: 500, timeout_ms: 2000 }
    victorialogs: { url: "http://127.0.0.1:9428" }
`)
	if hasCode(diags, "lint_vl_nonobject_payload") {
		t.Fatalf("lint fired for a raw source not reaching VL: %+v", diags)
	}
}

// D4: lint_collector_batch_one fires only on sinks actually downstream of a
// file source; a sink fed by another branch is a different shape and must not
// be flagged.
func TestLintCollectorBatchOneOnlyFileDownstream(t *testing.T) {
	twoBranches := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lintcase }
sources:
  logs:
    file: { path: a.jsonl }
  tick:
    cron: { expression: "* * * * *" }
sinks:
  fromfile:
    depends_on: [logs]
    encoder: json
    %s
    file: { path: from-file.jsonl }
  fromcron:
    depends_on: [tick]
    encoder: json
    %s
    file: { path: from-cron.jsonl }
`
	// No trigger: the unbatched sink is on the cron branch; the file branch
	// is batched.
	_, diags := build(t, fmt.Sprintf(twoBranches,
		"batch: { size: 100, timeout_ms: 1000 }", ""))
	if hasCode(diags, "lint_collector_batch_one") {
		t.Fatalf("lint fired for an unbatched sink outside the file downstream: %+v", diags)
	}
	// Trigger: the unbatched sink is the file branch.
	_, diags = build(t, fmt.Sprintf(twoBranches,
		"", "batch: { size: 100, timeout_ms: 1000 }"))
	if !hasWarning(diags, "lint_collector_batch_one") {
		t.Fatalf("expected lint_collector_batch_one for the file-fed sink, got %+v", diags)
	}
}
