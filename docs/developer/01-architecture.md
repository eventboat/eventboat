---
title: "Architecture & package map"
order: 1
---

# Architecture & package map

Eventboat is a single Go binary that routes events: messages arrive from
sources (Kafka, HTTP, cron, file, SQL), flow through an explicit DAG of
transforms and filtered edges, and land at sinks — at-least-once, verifiable
before anything runs, and replayable after a crash. Pipelines are declared in
YAML, statically verified against plugin JSON Schemas and topology rules, and
then executed by an engine whose entire reliability model is pinned by seven
dedicated invariant tests. Predicates are CEL, transforms are Starlark (or
WASM), so there is no custom language to learn.

## The processing model

A pipeline is a three-section YAML document — `sources`, `transforms`,
`sinks` — joined by `from` edges into a DAG. The runtime model is:

1. **Admission**: every inbound message is durably spooled (SQLite, or
   in-memory with `--ephemeral`) *before* it becomes visible to the DAG
   (`internal/engine/engine.go`, `accept`).
2. **Execution**: the message fans out along matched edges into transforms
   and sinks. Each execution branch must reach a terminal state: sink ack,
   dead letter, filtered (zero matching edges or zero transform outputs), or
   an optional-edge drop.
3. **Commit**: a commit tracker counts outstanding branches per message; the
   checkpoint only advances over the contiguous committed prefix
   (`internal/engine/commit.go`).
4. **Recovery**: on restart the engine replays the spool beyond the
   checkpoint while pull sources resume from their committed watermarks —
   duplicate delivery, never loss.

Sources, transforms and sinks are plugins in the registry
([Plugin system](03-plugins.md)); edges carry predicates (CEL or CESQL) and
delivery policies (retries, backoff, timeout, required/optional)
([Configuration](04-config-pipeline.md)).

## Event lifecycle through the engine

```
 source node                 engine (internal/engine)                    store
 ------------   -----------------------------------------------------   --------
     │  emit          accept(msg)                                            │
     │  ──────────▶   admission gate: admitSem (high watermark,              │
     │                default 10_000 in flight; blocks the source)           │
     │                stamp meta (message_id, ingest_time, source)           │
     │                                          │                            │
     │                                          ├─ AppendSpool ────────────▶ │ seq
     │                                          │       (invariant 1: not    │
     │                                          │        visible before this)│
     │                                          ▼                            │
     │                                   commit.arrived(seq, src, srcSeq)    │
     │                                          │                            │
     │                                          ▼                            │
     │                 dispatchFrom: decode (codec) at entry; decode failure  │
     │                 = dead letter                                          │
     │                                          │                            │
     │                                          ▼                            │
     │                 fanOut: evaluate each outgoing edge predicate          │
     │                     │ matched                                          │
     │         ┌───────────────┴──────────────────┐                           │
     │         ▼ (transform node)                ▼ (sink node)               │
     │   runTransform worker               runSink: batch (size/timeout)     │
     │   Apply → 0 outputs = filtered      encode, order_key, Write          │
     │   Apply → N outputs = fan out       retries per edge delivery policy  │
     │   Apply error → retries then        exhausted: optional → drop,       │
     │   dead letter (backtrace)           required → dead letter            │
     │         │                                 │                           │
     │         └───────────────┬─────────────────┘                           │
     │                         ▼                                             │
     │                 commit.done(seq) per branch                           │
     │                         │                                             │
     │                         ▼                                             │
     │     commitTracker.advanceLocked: contiguous committed prefix          │
     │                         │                                             │
     │          onCommit: metrics, span end, admission slot release          │
     │          onAdvance: persistCheckpoint ── SetCheckpoint ─────────────▶ │
     │                       (source Commit watermarks ── SetSourceState ▶ │)
     │                       (spool retention trim ─ DeleteSpoolThrough ─▶ │)
```

## Package map

Every directory, what it owns, and what it must not reach into. The layering
is enforced by Go imports — each "must not" below is checkable with
`go list -deps`.

| Package | Owns | Must not reach into |
|---|---|---|
| `internal/config` | Typed pipeline config, the strict loader (`LoadBytes`), `${VAR}`/`${?VAR}`/`${constants.x}` substitution, `metadata.name` validation, sections/edges/hooks/parameters parsing | Nothing internal — a leaf package. It knows nothing of the registry or IR; plugin validity is decided later |
| `internal/ir` | The static IR: DAG nodes/edges, compiled CEL/CESQL predicates and Starlark programs, topology checks, plugin + codec resolution, lint, job semantics. `ir.Build` *is* verify | `engine`, `store`, anything runtime. IR is built before an engine exists |
| `internal/engine` | Spool admission, DAG execution, commit tracking, per-edge delivery retries, dead lettering, checkpointing, backpressure, transform/sink workers | Config parsing or expression compilation — it consumes a built `ir.Pipeline` only. It also never decodes payloads itself (codecs do) |
| `internal/registry` | The plugin registration model: four kinds, JSON Schema validation (santhosh-tekuri), version pins, the typed struct→schema generator, `Catalog` | Any host package. A leaf — plugins register *into* it, it imports nothing internal |
| `internal/registry/builtin` | All compiled-in plugins: file/cron/http_server/kafka/sql sources; script/split/wasm transforms; file/http/kafka/drop sinks; json/raw/csv/avro/protobuf codecs | Anything beyond `registry` and `wasmhost` (for the wasm transform config) |
| `internal/lang/celhost` | CEL predicate host: env binding, compile, cost-limited eval | Nothing internal (leaf). It does not know about pipelines |
| `internal/lang/cesqlhost` | The CESQL edge dialect (official CloudEvents parser + the `data.*` rewrite) | Nothing internal (leaf) |
| `internal/lang/starhost` | The Starlark sandbox host: compile, frozen constants, lazy COW message bindings, step budget, backtraces | Nothing internal (leaf) |
| `internal/wasmhost` | wazero runtime host: wasip1 reactor compile, per-invoke budgets, guest ABI check, invokers | Nothing internal (leaf). `registry/builtin` adapts it |
| `internal/rpcplugin` | Out-of-process gRPC source/sink plugins: spawn, JSON handshake, auth metadata, restart supervision | `ir`/`engine`. It adapts `config.GrpcConfig` + `pkg/pluginv1` into registry-shaped sources/sinks |
| `internal/store` | The durable spine: spool, checkpoint, source states, dead letters, job history — SQLite (`modernc.org/sqlite`) and an in-memory implementation | Parsing or execution. It persists `registry.Message` but never interprets payloads |
| `internal/jobs` | Job pipelines: cron scheduling, catchup, overlap, run lifecycle, typed parameters (`cursor`/`now`), hooks, run history | Being a *caller* of the engine only — it must not reimplement admission or commit logic |
| `internal/ops` | The operations service: verify/test/explain/deploy/status/jobs/trigger/tail/dlq_query/dlq_replay/drain/pause/resume. The single implementation behind MCP and Admin REST | HTTP or protocol concerns. `admin`/`mcpserver`/CLI are thin shells over it |
| `internal/admin` | Admin REST + SSE + the embedded read-only UI + the security middleware (token, Host allowlist) | Business logic — everything delegates to `ops` |
| `internal/obs` | OpenTelemetry: one MeterProvider with Prometheus + OTLP readers, the instrument set, span helpers | Nothing internal (leaf); callers pass in pipeline/node names |
| `internal/explain` | Deterministic walkthroughs: symbolic and message-level traces, mermaid/ASCII topology | `engine` — explain runs compiled IR, never a live engine |
| `internal/lsp` | The language server: minimal hand-written JSON-RPC 2.0, diagnostics from the real verify path, completion, hover | Execution — it validates documents through `ops`, never runs pipelines |
| `internal/mcpserver` | The 14 MCP tools over stdio or Streamable HTTP (official Go SDK) | Business logic — thin shells over `ops` |
| `internal/runtimecfg` | Deployment-level `kind: Runtime` config: `storage.*`, `admin.*`, `mcp.*`, `telemetry.*` keys | Pipeline concerns; it never loads pipeline files |
| `internal/testkit` | Injection/capture/fault-injection primitives: `ManualSource`, `CaptureSink`, `FlakySink`, `StoreWrapper`, `RegisterFakeTransform` | Production behavior — nothing in `internal/` (outside tests) may import it |
| `internal/testrun` | The §3.2 contract-test runner: suite YAML → in-process real-engine runs with capture sinks | Only the public engine/testkit surfaces; it is itself consumed by `ops` and the CLI `test` verb |
| `internal/inttests` | Env-gated integration suites: `kafka/` (real broker via testcontainers) and `soak/` (long-run stability) | — test-only; skipped without their env vars |
| `pkg/pluginv1` | Generated gRPC stubs for `eventboat.plugin.v1` | Generated code — regenerate, never hand-edit. It lives in `pkg/` (not `internal/`) so third-party plugin modules can import it |
| `cmd/eventboat` | The CLI verbs: verify/test/run/trigger/jobs/explain/replay/repl/lsp/plugin/mcp, dispatch on lynx-go/commands, help goldens | New business logic — verbs delegate to `ops`, `testrun`, `explain` |
| `tools/` | Support tools. `tools/sitegen` (in progress) is the documentation site generator: a separate Go module, run with `go run ./tools/sitegen`, no CDN dependencies | Importing `internal/` — it is a separate module |
| `examples/` | Shipped pipelines (linear, branching, fan-in, job-sync, codecs), the third-party gRPC plugin example (`examples/plugins/ticker-source`, its own Go module), the VS Code launcher, the k8s manifest | — covered by the CI examples gate: all examples are verified and contract-tested |

## Design invariants

These are the properties the architecture is built around. Items 1–7 each
have a dedicated test (`TestInvariant_*` in `internal/engine/invariants_test.go`);
breaking any of them is a review blocker.

1. **Spool before visible.** A message must not become visible to the DAG
   until its spool append has succeeded. A failing append refuses the message;
   it never reaches any sink.
2. **Checkpoint only over committed.** The durable checkpoint may advance
   only across the contiguous prefix of messages whose every execution branch
   reached a terminal state.
3. **Crash = replay, never loss.** After a kill -9, the set replayed from the
   checkpoint is a superset of the uncommitted set. Duplicate delivery is the
   contract; loss is not.
4. **Dead-letter write failure blocks commit.** A dead letter that cannot be
   written is retried; the message stays uncommitted. Dead-letter unavailability
   slows the pipeline instead of losing messages.
5. **Optional-edge failure isolates.** A `required: false` edge failure
   terminates only its own branch and never blocks sibling branches from
   committing.
6. **Duplicate delivery is safe by design.** Re-delivery to an idempotent sink
   must be harmless; `meta.message_id` exists for dedup keys. This part is the
   user's documented responsibility.
7. **Cursor watermark follows commit.** Pull sources' watermarks advance only
   to the max cursor column of *committed* messages — job-pipeline resume
   correctness.

Project-level invariants that live outside the engine:

- **Verify-first for every write path.** `deploy`, the admin REST write
  surface, the MCP `deploy` tool and `eventboat run` all load and build
  through the same `config.LoadBytes` + `ir.Build` pipeline. `ops.Verify`
  returns diagnostics; `ops.Deploy` *fails* when verification fails. There is
  no bypass channel (`internal/ops/ops.go`).
- **The engine does not parse payloads.** Raw bytes plus a codec marker are
  the spooled truth (`internal/registry/registry.go`, `Message.Raw`); decoding
  and encoding are codec plugins. Replay stays compatible with codec upgrades
  because the spool never stores the decoded form.
- **Plugin isolation.** Out-of-process plugins are separate processes speaking
  gRPC on loopback with a one-shot token (`internal/rpcplugin`); WASM guests
  run in a capability sandbox with memory caps (`internal/wasmhost`); Starlark
  scripts run without while/recursion/I/O under a step budget
  (`internal/lang/starhost`). A wedged plugin degrades one worker or one
  pipeline, not the process.
- **Single binary.** Everything — engine, store, language hosts, admin UI,
  MCP, LSP — compiles into one static binary with CGO disabled (every driver
  is pure Go; SQLite via `modernc.org/sqlite`), see the `Dockerfile`.

## How a config becomes a running pipeline

The same path serves every entry point:

```
config.LoadBytes ──▶ ir.Build ──▶ engine.New ──▶ engine.Run(ctx)
     (diagnostics)     (static IR)   (plugins,       (replay, workers,
  cfg_* topo_* expr_*  + compiled     channels,        sources, commit)
  plugin_* codec_*     programs,      commit tracker)
  job_* lint_*         explain-safe
                       instances)
```

Entry points on top of this: `eventboat run --config` (one pipeline,
OTLP-only telemetry), `eventboat run --config-dir` (the multi-pipeline
daemon; every YAML in the directory loads, the admin surface serves on
`127.0.0.1:7788` by default), `ops.Deploy` (the admin/MCP write path —
verify-first, then drain-and-swap), and the one-shot verbs (`verify`,
`test`, `explain`, `replay`, `trigger`, `jobs`). Job pipelines
(`run.mode: job`) put `internal/jobs` between the config and the engine:
one engine per run, sharing one admission pool across `overlap: all` runs.

## Where to read next

- [Engine internals](02-engine.md) — the accept path, commit frontier,
  persistence and recovery in detail.
- [Plugin system & registry](03-plugins.md) — how to register, write and test
  plugins.
- [Configuration & diagnostics](04-config-pipeline.md) — the load pipeline and
  every diagnostic code.
- [Testing guide](07-testing.md) — where each guarantee is pinned.
