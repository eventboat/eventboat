package observability

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server exposes metrics, health, and debug endpoints.
type Server struct {
	cfg     config.ObservabilityConfig
	metrics *Metrics
	checker *Checker

	metricsMux *http.ServeMux
	healthMux  *http.ServeMux

	metricsSrv *http.Server
	healthSrv  *http.Server
	mu         sync.Mutex
}

func NewServer(cfg config.ObservabilityConfig, metrics *Metrics, checker *Checker, admin http.Handler) *Server {
	config.ApplyObservabilityDefaults(&cfg)
	s := &Server{
		cfg:     cfg,
		metrics: metrics,
		checker: checker,
	}
	s.metricsMux = http.NewServeMux()
	s.healthMux = http.NewServeMux()

	if cfg.Metrics.Enabled {
		s.metricsMux.Handle(cfg.Metrics.Path, promhttp.HandlerFor(s.metrics.Gatherer(), promhttp.HandlerOpts{}))
	}
	s.healthMux.HandleFunc(cfg.Health.Liveness, s.handleLiveness)
	s.healthMux.HandleFunc(cfg.Health.Readiness, s.handleReadiness)
	s.healthMux.HandleFunc("/debug/loglevel", s.handleLogLevel)
	if admin != nil {
		s.metricsMux.Handle("/admin/", admin)
		s.healthMux.Handle("/admin/", admin)
	}

	return s
}

// Start launches HTTP listeners. Metrics and health may share a port when configured equally.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	InitLogging(s.cfg.Logging)

	if s.cfg.Metrics.Enabled {
		addr := fmt.Sprintf(":%d", s.cfg.Metrics.Port)
		s.metricsMux.HandleFunc(s.cfg.Health.Liveness, s.handleLiveness)
		s.metricsMux.HandleFunc(s.cfg.Health.Readiness, s.handleReadiness)
		s.metricsMux.HandleFunc("/debug/loglevel", s.handleLogLevel)
		s.metricsSrv = &http.Server{Addr: addr, Handler: s.metricsMux}
		go func() {
			_ = s.metricsSrv.ListenAndServe()
		}()
	}

	healthEnabled := s.cfg.Health.Enabled || s.cfg.Metrics.Enabled
	if healthEnabled && s.cfg.Health.Port != s.cfg.Metrics.Port {
		addr := fmt.Sprintf(":%d", s.cfg.Health.Port)
		s.healthSrv = &http.Server{Addr: addr, Handler: s.healthMux}
		go func() {
			_ = s.healthSrv.ListenAndServe()
		}()
	}

	return nil
}

func (s *Server) SetPipelines(pipes ...PipelineHealth) {
	if s.checker != nil {
		s.checker.SetPipelines(pipes...)
	}
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	if s.metricsSrv != nil {
		if err := s.metricsSrv.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if s.healthSrv != nil {
		if err := s.healthSrv.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// MergeObservability picks the most capable observability config from loaded pipelines.
func MergeObservability(cfgs []*config.PipelineConfig) config.ObservabilityConfig {
	var merged config.ObservabilityConfig
	for _, cfg := range cfgs {
		if cfg == nil {
			continue
		}
		obs := cfg.Observability
		if obs.Metrics.Enabled {
			merged.Metrics.Enabled = true
		}
		if obs.Metrics.Port != 0 {
			merged.Metrics.Port = obs.Metrics.Port
		}
		if obs.Metrics.Path != "" {
			merged.Metrics.Path = obs.Metrics.Path
		}
		if obs.Health.Enabled {
			merged.Health.Enabled = true
		}
		if obs.Health.Port != 0 {
			merged.Health.Port = obs.Health.Port
		}
		if obs.Logging.Level != "" {
			merged.Logging.Level = obs.Logging.Level
		}
		if obs.Logging.Format != "" {
			merged.Logging.Format = obs.Logging.Format
		}
	}
	if !merged.Metrics.Enabled && merged.Metrics.Port == 0 {
		merged.Metrics.Enabled = true
	}
	config.ApplyObservabilityDefaults(&merged)
	return merged
}
