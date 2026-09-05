package ir

import (
	"strings"
	"testing"
)

// Transforms resolve through the registry exactly like sources and sinks
// (spec v1.19): unknown names, version pins and schema violations are verify
// findings with the same diagnostic codes.
func TestTransformPluginUnknown(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  t: { from: [in], mystery: {} }
sinks:
  out: { from: [t], file: { path: b } }
`)
	if !hasCode(diags, "plugin_unknown") {
		t.Fatalf("want plugin_unknown, got %+v", diags)
	}
}

// A grpc: block under a transform parses as a plugin named "grpc" (the
// out-of-process block syntax is source/sink-only): the plugin_unknown hint
// must say out-of-process transforms are future work, not send the user to
// the plugin catalog for a plugin that cannot exist.
func TestTransformGrpcBlockHintFutureWork(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  t:
    from: [in]
    grpc: { command: ["./t"], schema: "t/manifest.json" }
sinks:
  out: { from: [t], file: { path: b } }
`)
	for _, d := range diags {
		if d.Code == "plugin_unknown" && strings.Contains(d.Message, `"grpc"`) {
			if !strings.Contains(d.Hint, "future work") {
				t.Fatalf("hint = %q, want the future-work wording", d.Hint)
			}
			return
		}
	}
	t.Fatalf("expected plugin_unknown for grpc, got %+v", diags)
}

func TestTransformVersionPin(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  t:
    from: [in]
    version: 2
    script: |
      payload.x = 1
sinks:
  out: { from: [t], file: { path: b } }
`)
	if !hasCode(diags, "plugin_version_mismatch") {
		t.Fatalf("want plugin_version_mismatch, got %+v", diags)
	}
}

func TestTransformScriptCompileErrorKeepsCode(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  t:
    from: [in]
    script: |
      payload.x = undefined_thing
sinks:
  out: { from: [t], file: { path: b } }
`)
	if !hasCode(diags, "expr_starlark_compile") {
		t.Fatalf("want expr_starlark_compile, got %+v", diags)
	}
	for _, d := range diags {
		if d.Code == "expr_starlark_compile" &&
			(strings.Contains(d.Message, "starlark compile error") || strings.Contains(d.Message, "undefined")) {
			return
		}
	}
	t.Fatalf("compile diagnostic lost its detail: %+v", diags)
}

// The old hand-parsed wasm field diagnostics are now schema findings: the
// plugin's JSON Schema expresses every range and the allowlist.
func TestTransformWasmSchemaGates(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  negative:
    from: [in]
    wasm: { module: guests/heavy.wasm, timeout_ms: -1 }
  huge:
    from: [negative]
    wasm: { module: guests/heavy.wasm, max_memory_pages: 99999 }
  netcat:
    from: [huge]
    wasm: { module: guests/heavy.wasm, allow: [net] }
sinks:
  out: { from: [netcat], file: { path: b } }
`)
	schemaDiags := 0
	for _, d := range diags {
		if d.Code == "plugin_schema" {
			schemaDiags++
		}
	}
	if schemaDiags < 2 { // timeout_ms + max_memory_pages are schema violations; allow:[net] gates in the factory
		t.Fatalf("want schema findings, got %+v", diags)
	}
}

func TestTransformWasmMissingModuleIsCompileFinding(t *testing.T) {
	_, diags := build(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in: { file: { path: a } }
transforms:
  t: { from: [in], wasm: { module: does/not/exist.wasm } }
sinks:
  out: { from: [t], file: { path: b } }
`)
	if !hasCode(diags, "expr_wasm_compile") {
		t.Fatalf("want expr_wasm_compile, got %+v", diags)
	}
}
