package engine

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/observability"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/topology"
)

type Engine struct {
	reg         *registry.Registry
	pipelines   map[string]*Pipeline
	configPaths map[string]string
	reloading   map[string]bool
	reloadTasks map[string]*ReloadTask
	reloadMu    sync.Mutex
	metrics     *observability.Metrics
	obs         *observability.Server
	mu          sync.Mutex
}

func New(reg *registry.Registry) *Engine {
	if reg == nil {
		reg = registry.Default
	}
	return &Engine{
		reg:       reg,
		pipelines: make(map[string]*Pipeline),
		reloading: make(map[string]bool),
		metrics:   observability.NewMetrics(nil),
	}
}

func (e *Engine) Metrics() *observability.Metrics {
	return e.metrics
}

func (e *Engine) Load(ctx context.Context, ir *topology.TopologyIR) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.pipelines[ir.Name]; exists {
		return fmt.Errorf("pipeline %q already loaded", ir.Name)
	}
	p, err := NewPipeline(ctx, e.reg, ir, e.metrics)
	if err != nil {
		return err
	}
	e.pipelines[ir.Name] = p
	e.metrics.SetPipelineCount(len(e.pipelines))
	return nil
}

func (e *Engine) StartObservability(ctx context.Context, cfg config.ObservabilityConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	pipes := make([]observability.PipelineHealth, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	checker := observability.NewChecker(pipes...)
	adminMux := http.NewServeMux()
	NewAdminHandler(e).Register(adminMux)
	e.obs = observability.NewServer(cfg, e.metrics, checker, adminMux)
	return e.obs.Start(ctx)
}

func (e *Engine) StopObservability(ctx context.Context) error {
	e.mu.Lock()
	obs := e.obs
	e.mu.Unlock()
	if obs == nil {
		return nil
	}
	return obs.Stop(ctx)
}

func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	pipes := make([]*Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	e.mu.Unlock()
	for _, p := range pipes {
		if err := p.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	pipes := make([]*Pipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	e.mu.Unlock()
	var first error
	for _, p := range pipes {
		if err := p.Stop(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (e *Engine) Pipeline(name string) (*Pipeline, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.pipelines[name]
	return p, ok
}

func (e *Engine) PipelineCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pipelines)
}
