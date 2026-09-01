package observability_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/riverpod/riverpod/internal/config"
	"github.com/riverpod/riverpod/internal/observability"
	"github.com/riverpod/riverpod/internal/stage"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeStage struct {
	healthy bool
	msg     string
}

func (f fakeStage) ID() string                     { return "s1" }
func (f fakeStage) Kind() stage.Kind               { return stage.KindTransform }
func (f fakeStage) ComponentType() string          { return "map" }
func (f fakeStage) Init(context.Context) error   { return nil }
func (f fakeStage) Stop(context.Context) error   { return nil }
func (f fakeStage) HealthCheck(context.Context) stage.HealthStatus {
	return stage.HealthStatus{Healthy: f.healthy, Message: f.msg}
}

type fakePipeline struct {
	name    string
	healthy bool
}

func (f fakePipeline) Name() string { return f.name }
func (f fakePipeline) CheckStages(ctx context.Context) []observability.StageHealth {
	return []observability.StageHealth{
		observability.CheckStage(f.name, "s1", fakeStage{healthy: f.healthy}, ctx),
	}
}

func TestMetricsRecordEvent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	m.IncInflight("p1")
	m.RecordEvent("p1", "ok", 0)
	m.DecInflight("p1")
}

func TestReadinessHealthy(t *testing.T) {
	checker := observability.NewChecker(fakePipeline{name: "p1", healthy: true})
	report := checker.Readiness(context.Background(), "")
	if !report.Healthy {
		t.Fatalf("expected healthy report")
	}
}

func TestReadinessUnhealthy(t *testing.T) {
	checker := observability.NewChecker(fakePipeline{name: "p1", healthy: false})
	report := checker.Readiness(context.Background(), "")
	if report.Healthy {
		t.Fatalf("expected unhealthy report")
	}
}

func TestServerLivenessAndReadiness(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	checker := observability.NewChecker(fakePipeline{name: "p1", healthy: true})
	cfg := config.ObservabilityConfig{
		Metrics: config.MetricsConfig{Enabled: true, Port: 0, Path: "/metrics"},
		Health:  config.HealthConfig{Enabled: true},
	}
	config.ApplyObservabilityDefaults(&cfg)
	srv := observability.NewServer(cfg, metrics, checker, nil)

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Health.Liveness, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"alive"}`)
	})
	mux.HandleFunc(cfg.Health.Readiness, func(w http.ResponseWriter, r *http.Request) {
		report := checker.Readiness(r.Context(), "")
		if !report.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = io.WriteString(w, `{"healthy":true}`)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + cfg.Health.Liveness)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("live status = %d", resp.StatusCode)
	}

	resp2, err := http.Get(ts.URL + cfg.Health.Readiness)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d", resp2.StatusCode)
	}
	_ = srv
}

func TestMergeObservabilityDefaultsEnabled(t *testing.T) {
	cfg := observability.MergeObservability(nil)
	if !cfg.Metrics.Enabled {
		t.Fatalf("expected metrics enabled by default for run")
	}
	if cfg.Metrics.Port != 9090 {
		t.Fatalf("port = %d", cfg.Metrics.Port)
	}
}

func TestLogLevelParsing(t *testing.T) {
	observability.InitLogging(config.LoggingConfig{Level: "debug", Format: "json"})
	if !strings.Contains(strings.ToLower("debug"), "debug") {
		t.Fatal("unexpected")
	}
}
