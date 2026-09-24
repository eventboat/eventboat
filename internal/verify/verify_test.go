package verify

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

// Candidate 05 acceptance 4: two same-named pipeline files in different
// directories must resolve their relative grpc manifest against their own
// directory — the LSP/CLI/Deploy baseDir rule, not the process CWD.
func TestBaseDirResolvesGrpcManifest(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(dirA, "plugin", "manifest.json"),
		`{"kind":"source","name":"ext","version":1,"config_schema":{"type":"object","additionalProperties":false}}`)
	// Same relative path, different manifest: the wrong baseDir reads this
	// one and reports a name mismatch.
	writeFile(t, filepath.Join(dirB, "plugin", "manifest.json"),
		`{"kind":"source","name":"other","version":1,"config_schema":{"type":"object","additionalProperties":false}}`)

	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: basedir-grpc }
sources:
  in:
    ext: {}
    grpc: { command: ["./ext"], schema: "./plugin/manifest.json" }
sinks:
  out: { depends_on: [in], debug: {} }
`
	reg := testRegistry(t)
	resA := Bytes("pipeline.yaml", []byte(pipeline), dirA, reg, Options{})
	if !resA.OK || resA.Pipeline == nil {
		t.Fatalf("dirA should resolve its own manifest, got: %+v", resA.Diagnostics)
	}
	resB := Bytes("pipeline.yaml", []byte(pipeline), dirB, reg, Options{})
	if resB.Pipeline != nil {
		t.Fatal("dirB manifest declares a different name; verify must fail")
	}
	found := false
	for _, d := range resB.Diagnostics {
		if d.Code == "grpc_manifest_name" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want grpc_manifest_name for dirB, got %+v", resB.Diagnostics)
	}
}

// Candidate 05 acceptance 4 (wasm half): the transform module path resolves
// against the same baseDir; a bogus module in the other directory must fail.
func TestBaseDirResolvesWasmModule(t *testing.T) {
	guest, err := os.ReadFile(filepath.Join("..", "wasmhost", "testdata", "aggregate.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	dirA, dirB := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(dirA, "mod.wasm"), string(guest))
	writeFile(t, filepath.Join(dirB, "mod.wasm"), "not a wasm module")

	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: basedir-wasm }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], wasm: { module: "./mod.wasm", timeout_ms: 1000 } }
sinks:
  out: { depends_on: [t], debug: {} }
`
	reg := testRegistry(t)
	resA := Bytes("pipeline.yaml", []byte(pipeline), dirA, reg, Options{})
	if !resA.OK {
		t.Fatalf("dirA should compile the real module, got: %+v", resA.Diagnostics)
	}
	resB := Bytes("pipeline.yaml", []byte(pipeline), dirB, reg, Options{})
	if resB.Pipeline != nil {
		t.Fatal("dirB module is not wasm; verify must fail")
	}
	found := false
	for _, d := range resB.Diagnostics {
		if d.Code == "expr_wasm_compile" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want expr_wasm_compile for dirB, got %+v", resB.Diagnostics)
	}
}

// An explicit empty baseDir is the documented pure-text rule: no document
// directory exists, so relative paths resolve against the process CWD (here:
// the test's working directory).
func TestEmptyBaseDirResolvesAgainstCWD(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "plugin", "manifest.json"),
		`{"kind":"source","name":"ext","version":1,"config_schema":{"type":"object","additionalProperties":false}}`)
	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: basedir-cwd }
sources:
  in:
    ext: {}
    grpc: { command: ["./ext"], schema: "./plugin/manifest.json" }
sinks:
  out: { depends_on: [in], debug: {} }
`
	chdir(t, dir)
	reg := testRegistry(t)
	res := Bytes("submitted.yaml", []byte(pipeline), "", reg, Options{})
	if !res.OK {
		t.Fatalf("CWD resolution failed: %+v", res.Diagnostics)
	}
}

// One-shot and two-stage forms are the same composition: the merged
// diagnostics and the strict verdict do not depend on the entry.
func TestOneShotMatchesTwoStage(t *testing.T) {
	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: parity }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], wasm: { module: "../wasmhost/testdata/aggregate.wasm" } }
sinks:
  out: { depends_on: [t], debug: {} }
`
	reg := testRegistry(t)
	one := Bytes("p.yaml", []byte(pipeline), "", reg, Options{Strict: true})
	lr := LoadBytes("p.yaml", []byte(pipeline), "")
	if lr.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", lr.Diagnostics)
	}
	_, buildDiags := Build(lr.Pipeline, reg, Options{})
	two := append(lr.Diagnostics, buildDiags...)
	if len(one.Diagnostics) != len(two) {
		t.Fatalf("one-shot %d diags, two-stage %d: %+v vs %+v", len(one.Diagnostics), len(two), one.Diagnostics, two)
	}
	for i := range one.Diagnostics {
		if one.Diagnostics[i] != two[i] {
			t.Fatalf("diag %d differs: %+v vs %+v", i, one.Diagnostics[i], two[i])
		}
	}
	if one.OK || !one.Diagnostics.HasWarnings() {
		t.Fatalf("strict verdict should fail on the wasm kill-switch warning: OK=%v diags=%+v", one.OK, one.Diagnostics)
	}
	if !two.StrictOK(false) {
		t.Fatalf("non-strict verdict should pass: %+v", two)
	}

	// The same content without --strict has the same diagnostics and the
	// non-strict verdict the MCP/Admin surface reports.
	loose := Bytes("p.yaml", []byte(pipeline), "", reg, Options{})
	if !loose.OK || loose.Diagnostics.HasErrors() || !loose.Diagnostics.HasWarnings() {
		t.Fatalf("non-strict verdict: OK=%v diags=%+v", loose.OK, loose.Diagnostics)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
}
