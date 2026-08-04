# P2 Improvements Plan (Post-v2.0-alpha)

> Date: 2025-06-25
> Status: Tracked for v2.0-beta and later
> Reference: Code review against `edgestream-design.md` v2.0-draft

## P2 Issues from Code Review

These are deferred improvements that don't block alpha functionality but should be addressed before production readiness.

### 1. Metrics & Observability (v2.0-beta) — Sprint 1 done

**Design ref:** §10.1–§10.7

- [x] Implement `edgestream_*` Prometheus metrics endpoint (port 9090)
  - Pipeline: `edgestream_events_total`, `edgestream_event_latency_seconds`, `edgestream_inflight_events`
  - Stage: `edgestream_stage_duration_seconds`, `edgestream_stage_errors_total`
  - Edge: `edgestream_edge_buffer_size`, `edgestream_edge_dropped_total`
  - DLQ: `edgestream_dlq_enqueued_total`
- [x] OTLP tracing spans skeleton (pipeline → stage granularity; noop tracer)
- [x] Health endpoints: `/live` (liveness) + `/ready` (readiness with per-stage HealthCheck)
- [x] Structured JSON logging with dynamic level adjustment

### 2. Edge Disk Buffer (v2.0-beta) — Sprint 3 done

**Design ref:** §6.8

- [x] WAL format: `[msg_len(4B)][payload(msg_len)][meta_len(4B)][metadata(meta_len)]`
- [x] Segment files (64MB default)
- [x] Periodic fsync (500ms default)
- [x] Crash recovery: scan segments → rebuild queue
- [x] Overflow mode: memory → disk on backpressure
- [x] Offset tracking per segment

### 3. Hot Reload (v2.0-beta)

**Design ref:** §6.6

- [ ] `POST /admin/reload/{pipeline_name}` and `SIGHUP`
- [ ] Validate new config before stopping old pipeline
- [ ] Graceful drain + restart with new TopologyIR
- [ ] `GET /admin/reload/status/{task_id}` for async status
- [ ] 409 conflict for concurrent reloads

### 4. PollingSource Wrapper (v2.0)

**Design ref:** §4.3, §9.1

- [ ] Engine wrapper for `PollingSource` interface
- [ ] Timer-driven poll with exponential backoff on empty polls
- [ ] Pending queue management
- [ ] Backpressure skip when downstream full

### 5. error_mode Chain (v2.0) — Sprint 2 done

**Design ref:** §7.8

- [x] Propagate error_mode from engine → pipeline → transform
- [x] `propagate` (default): errors → delivery/DLQ
- [x] `ignore`: errors → false, no DLQ, metric logged
- [x] `silent`: same as ignore but no metric

### 6. WASM Transform (v2.0)

**Design ref:** §9.5

- [ ] wazero runtime integration
- [ ] `init(config_ptr, config_len)` optional export
- [ ] Safe host functions: `now()`, `uuid()`, `log()`
- [ ] timeout / memory_limit / fuel configuration
- [ ] No WASI file/network access

### 7. Code Quality Improvements (v2.0-beta)

- [ ] Extract duplicated `msgAdapter` from graph.go, map.go, filter.go, route.go into `eql` package
- [ ] Remove dead code (`_ = ctx` in matchEdges, `_ = id` in run loop)
- [ ] Add tests for engine package (runSource/runTransform/runSink integration)
- [ ] Add tests for topology.Validate edge cases
- [ ] Use `stage.Kind` constants in normalize.go (resolve circular import with shared constants package)

### 8. Feature Completeness (v2.0)

- [ ] `order: ordered` enforcing max_in_flight=1
- [ ] Per-partition ordering via `buffer.key`
- [ ] Route `action: copy` (fan-out to multiple routes)
- [ ] v2.2: VRL-style fallibility (`!`, `??`, `, err`)
- [ ] v2.2: Decision + Task steps (Envelope inspired)

## Priority Order

| Phase | Items |
|-------|-------|
| **v2.0-beta** | Metrics/Observability, Edge Disk Buffer, Hot Reload, Code Quality |
| **v2.0** | PollingSource, error_mode, WASM, Feature Completeness |
| **v2.1+** | gRPC plugins, K8s Operator, P1 components |
| **v2.2** | Fallibility, Decision/Task, per-partition ordering |
