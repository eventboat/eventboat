# Sprint 1: Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the last v2.0-alpha gap by shipping Prometheus metrics, health endpoints, and structured JSON logging wired into the engine runtime.

**Architecture:** New `internal/observability` package owns metrics registry, HTTP server mux, and slog setup. `Engine` holds a shared `*observability.Metrics` passed into each `Pipeline`; instrumentation hooks live in `dispatchFrom`, `runTransform`, `runSink`, and `deliverToDLQ`. OTLP tracing ships as a noop interface skeleton for Sprint 2+ expansion.

**Tech Stack:** Go 1.22, `github.com/prometheus/client_golang`, `log/slog`

## Global Constraints

- Metric prefix: `riverpod_`
- Default metrics port: `9090`, path `/metrics`
- Default health endpoints: `/live`, `/ready`
- Health `/live` does not query stages; `/ready` aggregates all Stage `HealthCheck`
- Admin/metrics mux independent of readiness semantics

---

### Task 1: Config + defaults

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/hocon.go`
- Modify: `internal/config/normalize.go`

- [x] Extend `ObservabilityConfig` with `Health` and `Logging` sub-structs
- [x] Add `ApplyObservabilityDefaults`

### Task 2: Metrics registry

**Files:**
- Create: `internal/observability/metrics.go`
- Create: `internal/observability/metrics_test.go`

- [x] Register Sprint 1 metric set with `prometheus` registry

### Task 3: HTTP server + health

**Files:**
- Create: `internal/observability/server.go`
- Create: `internal/observability/health.go`
- Create: `internal/observability/server_test.go`

- [x] `/metrics`, `/live`, `/ready`, `PUT /debug/loglevel`

### Task 4: Engine instrumentation

**Files:**
- Modify: `internal/engine/engine.go`
- Modify: `internal/engine/pipeline.go`
- Modify: `internal/engine/dispatch.go`
- Modify: `cmd/riverpod/main.go`

- [x] Wire metrics into pipeline hot paths
- [x] Start observability server from `riverpod run`

### Task 5: Verify

- [x] `go test ./...`
- [x] `go build ./cmd/riverpod`
