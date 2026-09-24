package ops

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 05 acceptance 6: Deploy verifies in memory once and the deploy
// directory is the config's baseDir — a relative wasm module is read from
// <data-dir>/pipelines (where the deployed file lives and jobs reload it),
// not from the process CWD.
func TestDeployResolvesRelativePathsFromDeployDir(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	pipelines := filepath.Join(dataDir, "pipelines")
	if err := os.MkdirAll(pipelines, 0o755); err != nil {
		t.Fatal(err)
	}
	guest, err := os.ReadFile(filepath.Join("..", "wasmhost", "testdata", "aggregate.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelines, "mod.wasm"), guest, 0o644); err != nil {
		t.Fatal(err)
	}

	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := New(Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)

	content := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: deploy-basedir }
sources:
  in: { file: { path: input.jsonl } }
transforms:
  t: { depends_on: [in], wasm: { module: "./mod.wasm", timeout_ms: 5000 } }
sinks:
  out: { depends_on: [t], debug: {} }
`
	summary, err := svc.Deploy(context.Background(), content)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if summary["pipeline"] != "deploy-basedir" {
		t.Fatalf("summary: %+v", summary)
	}
	if _, err := os.Stat(filepath.Join(pipelines, "deploy-basedir.yaml")); err != nil {
		t.Fatalf("deployed file missing: %v", err)
	}
}

// A relative grpc manifest is read from the deploy directory: the same text
// deployed from a CWD without that manifest must fail on the manifest
// CONTENT (it found the deploy dir's copy), not on a missing file.
func TestDeployReadsManifestFromDeployDir(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	manifestDir := filepath.Join(dataDir, "pipelines", "plugin")
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{"kind":"source","name":"other","version":1,"config_schema":{"type":"object","additionalProperties":false}}`
	if err := os.WriteFile(filepath.Join(manifestDir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := New(Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)

	_, err := svc.Deploy(context.Background(), `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: deploy-manifest }
sources:
  in:
    ext: {}
    grpc: { command: ["./ext"], schema: "./plugin/manifest.json" }
sinks:
  out: { depends_on: [in], debug: {} }
`)
	if err == nil {
		t.Fatal("manifest name mismatch must reject the deploy")
	}
	if !strings.Contains(err.Error(), "declares name") {
		t.Fatalf("deploy did not read the deploy dir's manifest: %v", err)
	}
	if strings.Contains(err.Error(), "no such file") {
		t.Fatalf("manifest resolved outside the deploy dir: %v", err)
	}
}

// Candidate 05 acceptance 6 (single parse): Deploy hands the pipeline it
// verified to the engine, so the transform factory runs exactly twice — once
// for the verify build, once for the engine's own instance. Before, Deploy
// verified, startManaged rebuilt the IR, and the engine instantiated again
// (three factory calls).
func TestDeploySingleParse(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := reg.RegisterTransform("counting", 1, `{"type":"object","additionalProperties":false}`, nil,
		func(cfg any, dir string) (registry.Transform, error) {
			calls.Add(1)
			return countingTransform{}, nil
		}); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := New(Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)

	if _, err := svc.Deploy(context.Background(), `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: deploy-once }
sources:
  in: { file: { path: input.jsonl } }
transforms:
  t: { depends_on: [in], counting: {} }
sinks:
  out: { depends_on: [t], debug: {} }
`); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("transform factory ran %d times, want 2 (verify + engine: Deploy must not rebuild)", got)
	}
}

// countingTransform is a no-op transform used to count factory invocations.
type countingTransform struct{}

func (countingTransform) Init(*registry.TransformEnv) error { return nil }
func (countingTransform) Apply(m *registry.Message) ([]*registry.Message, error) {
	return []*registry.Message{m}, nil
}
func (countingTransform) Close() error { return nil }

// A config that fails verify is rejected before anything is written: a
// failed Deploy leaves no half-deployed file or instance behind.
func TestDeployRejectedBeforeWrite(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := New(Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)

	_, err := svc.Deploy(context.Background(), `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: deploy-bad }
sources:
  in: { does-not-exist: {} }
sinks:
  out: { depends_on: [in], debug: {} }
`)
	if err == nil {
		t.Fatal("unknown plugin must be rejected")
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "pipelines", "deploy-bad.yaml")); !os.IsNotExist(statErr) {
		t.Fatalf("rejected deploy wrote the file (stat err = %v)", statErr)
	}
	if len(svc.Status()) != 0 {
		t.Fatalf("rejected deploy registered an instance: %+v", svc.Status())
	}
}
