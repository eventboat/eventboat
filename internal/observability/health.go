package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/riverpod/riverpod/internal/stage"
)

// StageHealth is a single stage readiness result.
type StageHealth struct {
	Pipeline string `json:"pipeline"`
	StageID  string `json:"stage_id"`
	Healthy  bool   `json:"healthy"`
	Message  string `json:"message,omitempty"`
}

// HealthReport aggregates readiness across pipelines.
type HealthReport struct {
	Healthy bool          `json:"healthy"`
	Stages  []StageHealth `json:"stages"`
}

// PipelineHealth exposes stage health checks for one pipeline.
type PipelineHealth interface {
	Name() string
	CheckStages(ctx context.Context) []StageHealth
}

// Checker aggregates pipeline health for /ready.
type Checker struct {
	mu        sync.RWMutex
	pipelines []PipelineHealth
}

func NewChecker(pipelines ...PipelineHealth) *Checker {
	return &Checker{pipelines: pipelines}
}

func (c *Checker) SetPipelines(pipelines ...PipelineHealth) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pipelines = pipelines
}

func (c *Checker) Readiness(ctx context.Context, pipelineFilter string) HealthReport {
	c.mu.RLock()
	pipelines := c.pipelines
	c.mu.RUnlock()

	report := HealthReport{Healthy: true}
	for _, p := range pipelines {
		if pipelineFilter != "" && p.Name() != pipelineFilter {
			continue
		}
		stages := p.CheckStages(ctx)
		for _, s := range stages {
			if !s.Healthy {
				report.Healthy = false
			}
			report.Stages = append(report.Stages, s)
		}
	}
	return report
}

func CheckStage(pipeline, stageID string, st stage.Stage, ctx context.Context) StageHealth {
	status := st.HealthCheck(ctx)
	return StageHealth{
		Pipeline: pipeline,
		StageID:  stageID,
		Healthy:  status.Healthy,
		Message:  status.Message,
	}
}

func writeHealth(w http.ResponseWriter, healthy bool, body any) {
	w.Header().Set("Content-Type", "application/json")
	if healthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeHealth(w, true, map[string]string{"status": "alive"})
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	filter := strings.TrimSpace(r.URL.Query().Get("pipeline"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	report := s.checker.Readiness(ctx, filter)
	writeHealth(w, report.Healthy, report)
}
