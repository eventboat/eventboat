# Sprint 2: Engine Constraints Implementation Plan

**Goal:** Wire `engine.max_workers`, `engine.max_inflight`, and `error_mode` propagation into the runtime.

**Status:** Implemented

- [x] `allocateTransformWorkers` caps transform goroutines per pipeline
- [x] `acquireInflight` / `beginMessageLifecycle` enforces max_inflight backpressure at source
- [x] `error_mode` chain: engine default → transform override for DSL eval and Process errors
- [x] Edge condition errors use upstream stage error_mode
