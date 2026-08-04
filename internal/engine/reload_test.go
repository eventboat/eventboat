package engine_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/engine"
	"github.com/edgesets/edgestream/internal/testutil"
	"github.com/edgesets/edgestream/internal/topology"
	_ "github.com/edgesets/edgestream/plugins/all"
)

func TestReloadPipeline(t *testing.T) {
	ctx := context.Background()
	eng := engine.New(nil)

	cfgPath := filepath.Join("..", "..", "testdata", "pipelines", "linear.yaml")
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	eng.SetConfigPath(ir.Name, cfgPath)
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}

	taskID, err := eng.BeginReload(ctx, ir.Name)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		task, ok := eng.ReloadTask(taskID)
		if ok && (task.Status == engine.ReloadSucceeded || task.Status == engine.ReloadFailed) {
			if task.Status != engine.ReloadSucceeded {
				t.Fatalf("reload failed: %s", task.Error)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = eng.Stop(stopCtx)
}

func TestAdminReloadConflict(t *testing.T) {
	ctx := context.Background()
	eng := engine.New(nil)
	cfgPath := filepath.Join("..", "..", "testdata", "pipelines", "linear.yaml")
	cfg, _ := config.LoadFile(cfgPath)
	ir, _ := topology.FromConfig(cfg)
	_ = eng.Load(ctx, ir)
	eng.SetConfigPath(ir.Name, cfgPath)

	eng.BeginReload(ctx, ir.Name)
	_, err := eng.BeginReload(ctx, ir.Name)
	if err != engine.ErrReloadInProgress {
		t.Fatalf("expected ErrReloadInProgress, got %v", err)
	}
}

func TestReloadKeepsOldPipelineOnBuildFailure(t *testing.T) {
	testutil.Register(nil)
	ctx := context.Background()
	eng := engine.New(nil)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "pipe.yaml")
	// cron ticks once a second so pipeline liveness is observable.
	good := []byte(`apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: live-pipe
steps:
  tick:
    source:
      type: cron
      config:
        schedule: "* * * * * *"
        payload: '{"tick":true}'
  reload-cap:
    depends_on: [tick]
    sink:
      type: test_capture
`)
	if err := os.WriteFile(cfgPath, good, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Load(ctx, ir); err != nil {
		t.Fatal(err)
	}
	eng.SetConfigPath(ir.Name, cfgPath)
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	oldPipe, ok := eng.Pipeline(ir.Name)
	if !ok {
		t.Fatal("pipeline missing after start")
	}
	cap, ok := testutil.CaptureSinkFor("reload-cap")
	if !ok {
		t.Fatal("capture sink missing")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(cap.Messages()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(cap.Messages()) == 0 {
		t.Fatal("pipeline produced no messages before reload")
	}

	// New config passes topology validation but cannot be instantiated
	// (unknown source plugin). The running pipeline must survive.
	bad := []byte(`apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: live-pipe
steps:
  src:
    source:
      type: no_such_source_plugin
  out:
    depends_on: [src]
    sink:
      type: drop
`)
	if err := os.WriteFile(cfgPath, bad, 0o644); err != nil {
		t.Fatal(err)
	}

	taskID, err := eng.BeginReload(ctx, ir.Name)
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		task, ok := eng.ReloadTask(taskID)
		if ok && (task.Status == engine.ReloadSucceeded || task.Status == engine.ReloadFailed) {
			if task.Status != engine.ReloadFailed {
				t.Fatal("reload with uninstantiable config should fail")
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	curPipe, ok := eng.Pipeline(ir.Name)
	if !ok || curPipe != oldPipe {
		t.Fatalf("failed reload replaced the running pipeline (ok=%v)", ok)
	}
	info, ok := eng.PipelineInfo(ir.Name)
	if !ok || info.State != engine.PipelineRunning {
		t.Fatalf("pipeline state = %q, want running after failed reload", info.State)
	}

	// The old pipeline must still be live, not just present in the map.
	before := len(cap.Messages())
	time.Sleep(1500 * time.Millisecond)
	if after := len(cap.Messages()); after <= before {
		t.Fatalf("pipeline dead after failed reload: %d messages before, %d after", before, after)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("old pipeline no longer stoppable after failed reload: %v", err)
	}
}

func TestAdminHandlerListPipelines(t *testing.T) {
	eng := engine.New(nil)
	mux := http.NewServeMux()
	engine.NewAdminHandler(eng).Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/admin/pipelines", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
