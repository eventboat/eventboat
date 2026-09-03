# Sprint 3: Edge Disk Buffer Implementation Plan

**Status:** Implemented

- [x] `internal/buffer/wal.go` — ERV1 WAL record encode/decode
- [x] `internal/buffer/disk.go` — segmented DiskWAL with offset tracking + periodic fsync
- [x] `internal/buffer/edge.go` — per-edge `EdgeInbound` (memory / disk / overflow)
- [x] Engine wiring — per-edge buffers in `graph.go`, dispatch via `sendToInbound`
- [x] Graceful shutdown — close edge buffers (fsync) after stage drain, before sink flush
