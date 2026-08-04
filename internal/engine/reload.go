package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/observability"
	"github.com/edgesets/edgestream/internal/topology"
	"github.com/google/uuid"
)

var ErrReloadInProgress = errors.New("pipeline reload already in progress")

type PipelineState string

const (
	PipelineRunning   PipelineState = "running"
	PipelineStopped   PipelineState = "stopped"
	PipelineReloading PipelineState = "reloading"
)

type ReloadStatus string

const (
	ReloadPending   ReloadStatus = "pending"
	ReloadRunning   ReloadStatus = "running"
	ReloadSucceeded ReloadStatus = "succeeded"
	ReloadFailed    ReloadStatus = "failed"
)

type ReloadTask struct {
	ID         string       `json:"id"`
	Pipeline   string       `json:"pipeline,omitempty"`
	Status     ReloadStatus `json:"status"`
	Error      string       `json:"error,omitempty"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
}

type PipelineInfo struct {
	Name       string        `json:"name"`
	State      PipelineState `json:"state"`
	StageCount int           `json:"stage_count"`
	EdgeCount  int           `json:"edge_count"`
	ConfigPath string        `json:"config_path,omitempty"`
}

func (e *Engine) SetConfigPath(name, path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.configPaths == nil {
		e.configPaths = make(map[string]string)
	}
	e.configPaths[name] = path
}

func (e *Engine) ConfigPath(name string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.configPaths[name]
}

func (e *Engine) ListPipelines() []PipelineInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]PipelineInfo, 0, len(e.pipelines))
	for name, p := range e.pipelines {
		out = append(out, e.pipelineInfoLocked(name, p))
	}
	return out
}

func (e *Engine) PipelineInfo(name string) (PipelineInfo, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.pipelines[name]
	if !ok {
		return PipelineInfo{}, false
	}
	return e.pipelineInfoLocked(name, p), true
}

func (e *Engine) pipelineInfoLocked(name string, p *Pipeline) PipelineInfo {
	state := PipelineStopped
	if p.started.Load() {
		state = PipelineRunning
	}
	if e.reloading[name] {
		state = PipelineReloading
	}
	return PipelineInfo{
		Name:       name,
		State:      state,
		StageCount: len(p.ir.Stages),
		EdgeCount:  len(p.ir.Edges),
		ConfigPath: e.configPaths[name],
	}
}

func (e *Engine) ReloadTask(id string) (ReloadTask, bool) {
	e.reloadMu.Lock()
	defer e.reloadMu.Unlock()
	t, ok := e.reloadTasks[id]
	if !ok {
		return ReloadTask{}, false
	}
	cp := *t
	return cp, true
}

// BeginReload validates config from disk and starts an async pipeline reload.
func (e *Engine) BeginReload(ctx context.Context, name string) (string, error) {
	e.mu.Lock()
	path, ok := e.configPaths[name]
	if !ok {
		e.mu.Unlock()
		return "", fmt.Errorf("pipeline %q not found", name)
	}
	if e.reloading[name] {
		e.mu.Unlock()
		return "", ErrReloadInProgress
	}
	e.reloading[name] = true
	e.mu.Unlock()

	ir, err := e.loadIRFromPath(path)
	if err != nil {
		e.clearReloading(name)
		return "", err
	}
	if ir.Name != name {
		e.clearReloading(name)
		return "", fmt.Errorf("config %q defines pipeline %q, expected %q", path, ir.Name, name)
	}

	taskID := uuid.NewString()
	task := &ReloadTask{
		ID:        taskID,
		Pipeline:  name,
		Status:    ReloadPending,
		StartedAt: time.Now().UTC(),
	}
	e.storeTask(task)

	go e.runReload(context.Background(), taskID, name, ir)
	return taskID, nil
}

// BeginReloadAll reloads every loaded pipeline sequentially in one async task.
func (e *Engine) BeginReloadAll(ctx context.Context) (string, error) {
	e.mu.Lock()
	names := make([]string, 0, len(e.pipelines))
	for name := range e.pipelines {
		if e.reloading[name] {
			e.mu.Unlock()
			return "", ErrReloadInProgress
		}
		names = append(names, name)
	}
	for _, name := range names {
		e.reloading[name] = true
	}
	e.mu.Unlock()

	for _, name := range names {
		path := e.ConfigPath(name)
		if path == "" {
			e.clearReloading(name)
			return "", fmt.Errorf("pipeline %q has no config path for reload", name)
		}
		if _, err := e.loadIRFromPath(path); err != nil {
			for _, n := range names {
				e.clearReloading(n)
			}
			return "", err
		}
	}

	taskID := uuid.NewString()
	task := &ReloadTask{
		ID:        taskID,
		Status:    ReloadPending,
		StartedAt: time.Now().UTC(),
	}
	e.storeTask(task)

	go e.runReloadAll(context.Background(), taskID, names)
	return taskID, nil
}

func (e *Engine) loadIRFromPath(path string) (*topology.TopologyIR, error) {
	cfg, err := config.LoadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return ir, nil
}

func (e *Engine) runReloadAll(ctx context.Context, taskID string, names []string) {
	e.updateTask(taskID, ReloadRunning, "")
	var firstErr error
	for _, name := range names {
		path := e.ConfigPath(name)
		ir, err := e.loadIRFromPath(path)
		if err != nil {
			firstErr = err
			break
		}
		if err := e.swapPipeline(ctx, name, ir); err != nil && firstErr == nil {
			firstErr = err
		}
		e.clearReloading(name)
	}
	if firstErr != nil {
		e.updateTask(taskID, ReloadFailed, firstErr.Error())
		return
	}
	e.updateTask(taskID, ReloadSucceeded, "")
	e.refreshHealthChecker()
}

func (e *Engine) runReload(ctx context.Context, taskID, name string, ir *topology.TopologyIR) {
	defer e.clearReloading(name)

	e.updateTask(taskID, ReloadRunning, "")
	if err := e.swapPipeline(ctx, name, ir); err != nil {
		e.updateTask(taskID, ReloadFailed, err.Error())
		return
	}
	e.updateTask(taskID, ReloadSucceeded, "")
	e.refreshHealthChecker()
}

func (e *Engine) swapPipeline(ctx context.Context, name string, ir *topology.TopologyIR) error {
	e.mu.Lock()
	old, ok := e.pipelines[name]
	e.mu.Unlock()
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}

	drainTimeout := 30 * time.Second
	if ir.Engine.DrainTimeout != "" {
		if d, err := time.ParseDuration(ir.Engine.DrainTimeout); err == nil {
			drainTimeout = d
		}
	}
	stopCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	err := old.Stop(stopCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("stop pipeline %q: %w", name, err)
	}

	newPipe, err := NewPipeline(ctx, e.reg, ir, e.metrics)
	if err != nil {
		return fmt.Errorf("build pipeline %q: %w", name, err)
	}
	if err := newPipe.Start(ctx); err != nil {
		return fmt.Errorf("start pipeline %q: %w", name, err)
	}

	e.mu.Lock()
	e.pipelines[name] = newPipe
	e.metrics.SetPipelineCount(len(e.pipelines))
	e.mu.Unlock()
	return nil
}

func (e *Engine) clearReloading(name string) {
	e.mu.Lock()
	delete(e.reloading, name)
	e.mu.Unlock()
}

func (e *Engine) storeTask(task *ReloadTask) {
	e.reloadMu.Lock()
	if e.reloadTasks == nil {
		e.reloadTasks = make(map[string]*ReloadTask)
	}
	e.reloadTasks[task.ID] = task
	e.reloadMu.Unlock()
}

func (e *Engine) updateTask(id string, status ReloadStatus, errMsg string) {
	e.reloadMu.Lock()
	defer e.reloadMu.Unlock()
	task, ok := e.reloadTasks[id]
	if !ok {
		return
	}
	task.Status = status
	task.Error = errMsg
	if status == ReloadSucceeded || status == ReloadFailed {
		now := time.Now().UTC()
		task.FinishedAt = &now
	}
}

func (e *Engine) refreshHealthChecker() {
	e.mu.Lock()
	obs := e.obs
	pipes := make([]observability.PipelineHealth, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		pipes = append(pipes, p)
	}
	e.mu.Unlock()
	if obs != nil {
		obs.SetPipelines(pipes...)
	}
}
