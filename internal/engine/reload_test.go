package engine_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/engine"
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
