# Eventboat

**Agent-native event routing data plane.** Pipelines as code, verified by
machines, operated by agents.

Eventboat is a single Go binary that routes events between systems. You
declare a pipeline in YAML — sources, transforms and sinks joined by `from`
into an explicit DAG — and Eventboat executes it durably: events come in
(Kafka / HTTP / cron / files / SQL), flow through filters, maps and routes,
and land at their destinations at-least-once, verifiably, replayably.
Predicates are plain [CEL](https://github.com/google/cel-go) (the Kubernetes
expression language) and transforms are
[Starlark](https://github.com/google/starlark-go) (a Python dialect): no
custom language to learn, and the largest possible training corpus for the
agents that write your pipelines.

**Docs:** <https://eventboat.github.io/eventboat/> — developer guides under
[docs/developer/](docs/developer/), reference docs for
[plugins](docs/plugins.md), [WASM](docs/wasm.md) and
[codecs](docs/codecs.md).

> **Status:** v0.3.0, pre-1.0 — the config surface is stable in practice,
> the API may still shift between minor versions. See
> [CHANGELOG.md](CHANGELOG.md).
> 中文说明见 [README_ZH.md](README_ZH.md). License: Apache-2.0.

## Why Eventboat

- **Verified before it runs.** `eventboat verify` is static, deterministic
  and side-effect free: plugin JSON Schemas, topology rules, CEL/Starlark
  compilation, lint. Broken configs never reach production — and the same
  verify powers the CLI, the LSP diagnostics and the MCP tool.
- **Testable like code.** Contract tests run the *real* in-process engine
  with fixture injection and capture, so `eventboat test` gates pipeline
  changes in CI exactly like unit tests gate code.
- **Reliable by construction.** Every inbound message is durably spooled
  (SQLite, pure Go, no CGO) *before* it becomes visible to the DAG; a commit
  tracker advances the checkpoint only over messages that reached a terminal
  state; crash recovery replays the spool beyond the checkpoint. The
  contract is at-least-once: duplicate delivery is possible, loss is not.
  Each of the seven reliability invariants has a dedicated test
  (`TestInvariant_*` in [internal/engine](internal/engine/invariants_test.go)).
- **Explainable and replayable.** `explain` walks a pipeline symbolically or
  executes it against a sample message (scripts included); `replay`
  reinjects dead letters, spool windows or failed job runs — with a
  `--dry-run` that tells you what *would* happen first.
- **Agent-native operations.** An MCP server (stdio + HTTP, 14 tools), an
  Admin REST API with SSE and a read-only console, an LSP for editors, and
  `--json` on every command — all thin shells over one ops service, so
  agents, editors and humans see identical truth.
- **An extension ladder, not a plugin cliff.** Compile-in Go plugins for
  custom builds, out-of-process gRPC plugins in any language, WASM
  transforms sandboxed by wazero, and a CESQL edge dialect — each tier
  exists for performance or dependencies, never for "complex logic".
- **One boring binary.** Pure Go, CGO-free, distroless container, and
  linux/amd64 + arm64 images on GHCR.

## Features

**Pipeline model** — a pipeline is three sections joined by `from`; the
plugin name is the key:

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: order-branching }

constants:
  vip_threshold: 10000

sources:
  ingest:
    decoder: json
    file: { path: input/events.jsonl }

transforms:
  enrich:
    from: [ingest]
    script: |
      payload.total = payload.price * payload.qty
      if payload.total > constants.vip_threshold:
          meta.tier = "vip"
      else:
          meta.tier = "basic"

sinks:
  eu-out:
    from: { enrich: { when: 'payload.region == "eu"' } }   # CEL predicate
    encoder: json
    file: { path: output/eu.jsonl }
```

- **Sources:** `kafka`, `http_server`, `cron`, `file`, `sql` (MySQL /
  PostgreSQL / SQLite, keyset pagination with resumable watermarks).
- **Transforms:** `script` (Starlark), `split` (one message per array
  element), `wasm` (see below) — or your own registered transform.
- **Sinks:** `kafka`, `http`, `file`, `drop`.
- **Codecs:** `json`, `raw`, `csv`, `avro`, `protobuf` — declared once under
  `codecs:` and referenced by name on any node (`decoder:` / `encoder:`).
- **Routing:** CEL predicates on every edge, fan-in / fan-out, per-node
  `workers`, sink batching, `order_key`, explicit delivery policies.
- **Failure semantics:** dead letters with script backtraces and reason
  classes, retries per edge policy, fan-out with zero matching edges commits
  as *filtered* (counted, never silently dropped).

**Job pipelines** — scheduled and triggered batch runs on the same engine:
cron schedules with catch-up windows, typed run parameters
(`${parameters.x}`), overlap policies (`skip` / `latest` / `all`), run
history in the same store, success/failure hooks, `cursor` watermarks for
incremental SQL sync, and `eventboat trigger` / `eventboat jobs` to backfill
and inspect.

**Verification gates**

| Gate | Command | What it does |
|---|---|---|
| Verify | `eventboat verify` | Static, deterministic, zero side effects: schemas, topology, CEL/Starlark compilation, lint |
| Contract tests | `eventboat test` | The real engine in-process: inject fixtures at any node, capture at any sink, assert on outputs or dead letters |
| Explain | `eventboat explain` | Symbolic walkthrough, message-level dry run against a sample, topology as mermaid + ASCII |
| Replay | `eventboat replay` | Reinject dead letters / spool windows / failed job runs; `--dry-run` previews, `--delete` prunes after success |
| Repl | `eventboat repl` | One-shot or interactive CEL/Starlark evaluation against a sample message |

**Agent & editor surface**

- **MCP:** `eventboat mcp --stdio` (agent hosts spawn this) or `--http`
  — 14 tools: catalog, verify, test, explain, deploy, status, jobs, trigger,
  tail, dlq_query, dlq_replay, drain, pause, resume.
- **Admin REST + SSE + read-only console** on the daemon's admin listener
  (default `127.0.0.1:7788`); bearer-token auth, and non-loopback binds
  refuse to start without a token.
- **LSP:** `eventboat lsp` — diagnostics from the real verify pipeline,
  completion over the plugin catalog and schemas, hover docs. Minimal VS
  Code launcher in [examples/editors/vscode](examples/editors/vscode).
- **Schema export:** `eventboat plugin schema --all --dir schemas/` — an
  offline JSON Schema bundle of every plugin for IDEs and agents.

**Extension ladder**

1. **Compile-in plugins** — `pkg/plugin` is the plugin ABI: register a
   source/transform/sink/codec from `init()` and let a four-line `main`
   delegate to the root package's `RunCLI`. Your plugin is a first-class
   citizen of verify, `plugin catalog`, LSP and MCP. Reference:
   [examples/custom-build](examples/custom-build).
2. **Out-of-process gRPC plugins** — any language that can serve gRPC:
   a static manifest keeps verify side-effect free, a stdout JSON handshake
   authenticates the runtime connection, `version:` pins fail loudly on
   drift, and `grpc.restart` chooses fast-fail vs. supervised respawning.
   Protocol + SDK docs: [docs/plugins.md](docs/plugins.md); third-party
   example: [examples/plugins/ticker-source](examples/plugins/ticker-source).
3. **WASM transforms** — wazero-hosted, capability-sandboxed, per-invoke
   wall-clock + memory budgets; guests build with the standard Go toolchain
   (`GOOS=wasip1`). For heavy per-message computation where Starlark is too
   slow. Docs: [docs/wasm.md](docs/wasm.md).
4. **CESQL edge dialect** — `when: { lang: cesql, expr: ... }` reuses the
   official CloudEvents parser; the official TCK runs 100% in CI.

**Observability** — OpenTelemetry with dual export: Prometheus at
`/metrics` and OTLP push; ~28 `eventboat_` metrics (throughput, commits,
dead letters by reason, backpressure, spool depth, job counters, latency
histograms), one span per job run, optional per-message spans, and
`telemetry.redact` masking on the `tail` surface.

**Security** — optional bearer token on the whole admin surface
(`--admin-token` > `EVENTBOAT_ADMIN_TOKEN` > Runtime config); non-loopback
admin binds refuse to start without one; loopback binds enforce a Host
allowlist against DNS rebinding.

## Architecture

```
                YAML (sources/transforms/sinks + from)
                                  │
                          loader ─┴─ ${VAR}/${?VAR}/${constants.*}/${parameters.*}
                                  │                           strict whitelists
                          verify  ─┴─ plugin JSON Schemas, topology rules,
                                  │   CEL + Starlark compilation, lint
                                  │
                        Static IR (DAG + compiled programs)
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                          ▼
   Engine (per pipeline)                              Ops service
   source ─▶ spool (SQLite) ─▶ in-memory DAG ─▶ sinks    │  ├─ CLI (--json)
                  │              commit tracking          │  ├─ MCP (stdio/HTTP)
                  └─ checkpoint ◀──── terminal states ────┘  ├─ Admin REST + SSE + UI
                        (sink ok / dead letter / filtered)   └─ LSP
```

- **Load → verify → IR.** The loader is strict (unknown fields are errors)
  and performs one pass of `${...}` substitution from environment,
  `constants:`, and (job pipelines only) trigger `parameters:`. Verify
  compiles everything — plugin configs against their JSON Schemas, every
  CEL/Starlark/CESQL program, the graph topology — into a static IR. If
  verify passes, the pipeline is fully known before a single message flows.
- **The engine.** Each pipeline instance spools every inbound message to
  SQLite before the DAG sees it, executes the DAG in memory, and a commit
  tracker counts each message's execution branches to terminal states (sink
  written / dead-lettered / filtered). The checkpoint advances only over
  committed prefixes; crash recovery replays the spool beyond the checkpoint
  while pull sources resume from their committed watermarks. Backpressure
  propagates from sinks to sources through the spool admission gate.
- **One ops service.** `internal/ops` is the entire operations surface; the
  CLI's `--json`, the MCP tools and the Admin REST endpoints share the same
  Go structs and JSON shapes. There is no bypass around verify: `deploy`
  fails when verification fails.
- **Storage.** SQLite (`modernc.org/sqlite`, pure Go) carries the spool,
  checkpoints, dead letters and job history; `--ephemeral` swaps in the
  in-memory stores for local development. Spool retention is bounded by
  `storage.spool_retention` in the Runtime config.

## Usage

### Build and run

```bash
go build -o eventboat ./cmd/eventboat   # or: docker build -t eventboat .

eventboat verify --config examples/linear/pipeline.yaml
eventboat test examples/linear          # contract tests
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral        # in-memory, for local dev
eventboat run --config-dir pipelines/             # multi-pipeline daemon + admin API
```

### Verify-first workflow (for humans and agents)

```bash
eventboat --json verify --config pipeline.yaml    # structured diagnostics, CI-friendly

eventboat explain --config pipeline.yaml --topology                 # mermaid + ASCII
eventboat explain --config pipeline.yaml --message sample.json      # executes scripts
eventboat replay --config pipeline.yaml --dlq --since 2h --dry-run  # preview
eventboat replay --config pipeline.yaml --dlq --where 'payload.region == "eu"' --delete
```

### Job pipelines

```bash
eventboat run --config examples/job-sync/pipeline.yaml     # scheduler + catch-up
eventboat trigger --config examples/job-sync/pipeline.yaml \
  --parameters '{"from":"2026-09-01T00:00:00Z","to":"2026-09-02T00:00:00Z"}'
eventboat jobs list --config examples/job-sync/pipeline.yaml
eventboat jobs show <run-id> --config examples/job-sync/pipeline.yaml
```

### Agents and editors

```bash
eventboat mcp --stdio          # MCP over stdio — point your agent host here
eventboat mcp --http           # MCP + Admin REST + SSE + read-only UI
eventboat lsp                  # language server over stdio
eventboat plugin catalog       # everything registered, with versions
eventboat plugin schema --all --dir schemas/
```

### Contract tests

A test suite injects fixtures into the real engine and asserts on captures
or dead letters:

```yaml
suite: order-branching
pipeline: ../pipeline.yaml
cases:
  - name: eu-order-routed-to-eu-only
    inject: { at: ingest, messages: [fixtures/eu-order.json] }
    expect:
      capture:
        at: eu-out
        messages:
          - payload.total: 12000     # subset match
            meta.tier: vip
  - name: malformed-json-to-dlq
    inject: { at: ingest, raw: "{not json" }
    expect:
      dlq: { count: 1, reason_contains: decode }
```

### Custom builds (compile-time plugins)

```go
package main

import (
	_ "example.com/myproject/myecho" // registers via pkg/plugin in init()
	"github.com/eventboat/eventboat"
)

func main() { eventboat.RunCLI() }
```

Build this `main` and you have a private `eventboat` binary where `myecho`
is indistinguishable from a built-in — verify, `plugin catalog`, LSP and MCP
all know it. A runnable reference lives in
[examples/custom-build](examples/custom-build).

### Containers and Kubernetes

```bash
docker build -t eventboat:dev .    # CGO_ENABLED=0 → distroless/static, nonroot
docker run --rm -v "$PWD/examples/linear:/work" -w /work eventboat:dev \
  run --config /work/pipeline.yaml
```

CI publishes `ghcr.io/eventboat/eventboat` (`:main`, `:sha-<short>`, version
tags; linux/amd64 + arm64). `/pipelines` and `/data` are the conventional
mount points; a ready Deployment manifest is in
[examples/k8s/deployment.yaml](examples/k8s/deployment.yaml) with notes in
[docs/k8s.md](docs/k8s.md).

## Repository layout

```
eventboat.go           RunCLI — library entry point for custom builds
cmd/eventboat/         the shipped binary (thin main over internal/cli)
internal/cli/          CLI verbs: verify / test / run / trigger / jobs / explain / replay / repl / lsp / plugin / mcp
internal/config/      typed config, strict loader, ${...} substitution, codecs: declarations
internal/ir/          static IR: DAG, compiled CEL/Starlark/CESQL, topology, lint
internal/lang/        celhost / cesqlhost / starhost (the language sandboxes)
internal/wasmhost/    wazero host: capability sandbox, per-invoke budgets
internal/rpcplugin/   gRPC plugin host: spawn, handshake, source/sink adapters
internal/engine/      spool admission, DAG execution, commit tracking, DLQ, pull sources
internal/jobs/        job runtime: scheduler, catch-up, overlap, run lifecycle, hooks
internal/store/       SQLite + in-memory spool/checkpoint/dead-letter/job-history stores
internal/registry/    plugin registration: JSON Schemas from typed config structs + ABI versions
internal/registry/builtin/  kafka/http_server/cron/file/sql sources, script/split/wasm transforms, kafka/http/file/drop sinks, json/raw/csv/avro/protobuf codecs
internal/lsp/         language server (JSON-RPC 2.0 over stdio)
internal/explain/     deterministic walkthroughs + topology rendering
internal/ops/         the operations service behind MCP and Admin REST
internal/mcpserver/   MCP tools (official Go SDK)
internal/admin/       Admin REST + SSE + embedded read-only console
internal/obs/         OpenTelemetry: dual export, metrics, spans
proto/, pkg/pluginproto/   the out-of-process plugin wire protocol (eventboat.plugin.v1)
pkg/plugin/           the compile-time plugin ABI (RegisterSource/Transform/Sink/Codec)
docs/                 plugins.md, wasm.md, codecs.md, k8s.md + developer guides
examples/             linear, branching, fanin, job-sync, codecs, custom-build, plugins/, editors/vscode, k8s
```

## Development

```bash
go build ./...
go test ./...          # includes the seven TestInvariant_* reliability tests
go test -race ./...

# integration suites (gated by env; skipped locally)
EVENTBOAT_KAFKA_TEST=1 go test ./internal/inttests/kafka/   # needs Docker
EVENTBOAT_SOAK_TEST=1 EVENTBOAT_SOAK_DURATION=2m go test ./internal/inttests/soak/

bash scripts/bench-gate.sh   # the loose performance gate CI enforces
```

Contributing and architecture deep-dives live in the
[developer guides](docs/developer/); design history is in
[CHANGELOG.md](CHANGELOG.md) and the tagged design documents in the repo
root.

## License

[Apache-2.0](LICENSE)
