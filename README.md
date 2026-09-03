# Eventboat

**Agent-native event routing data plane.** Pipelines as code, verified by
machines, operated by agents.

Eventboat is a single Go binary: events come in (Kafka / HTTP / cron / file),
flow through an explicit DAG (filter, map, route), and land at their
destinations — at-least-once, verifiable, replayable. Predicates are plain
[CEL](https://github.com/google/cel-go) (the Kubernetes expression language),
transforms are [Starlark](https://github.com/google/starlark-go) (a Python
dialect): zero custom language to learn, maximal training corpus for the
agents that write your pipelines.

> **Status: v3 POC** (milestone M1 of [redesign-v3.md](redesign-v3.md)). The
> v2 implementation is archived under [legacy/](legacy/) and is not
> compatible. The pre-implementation design review lives in
> [redesign-v3-review.md](redesign-v3-review.md) (verdict: pass, 13 findings).
> License: Apache-2.0. 中文说明见 [README_ZH.md](README_ZH.md).

## How it works

```
                YAML (sources/transforms/sinks + from)
                                  │
                          loader ─┴─ ${VAR} substitution, strict whitelists
                                  │
                          verify  ─┴─ plugin JSON Schemas, topology rules,
                                  │   CEL + Starlark compilation, lint
                                  │
                        Static IR (DAG + compiled programs)
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                          ▼
   Engine (per pipeline)                              CLI / future MCP
   source ─▶ spool (SQLite) ─▶ in-memory DAG ─▶ sinks
                  │              settle tracking      │
                  └─ checkpoint ◀──── terminal states ┘
                        (sink ok / dead letter / optional drop)
```

Reliability model (redesign-v3.md §6.2): every inbound message is durably
spooled *before* it becomes visible to the DAG; a settle tracker counts
execution branches to terminal states; the checkpoint only advances over
settled messages; crash recovery replays the spool beyond the checkpoint.
Each of the seven invariants has a dedicated test (`TestInvariant_*` in
[internal/engine](internal/engine/invariants_test.go)).

## Quick start

```bash
# build
go build -o eventboat ./cmd/eventboat

# gate 1: verify (static, deterministic, zero side effects)
eventboat verify --config examples/linear/pipeline.yaml
eventboat --json verify --config examples/branching/pipeline.yaml   # for CI/agents

# gate 2: contract tests — in-process real engine, fixture injection, capture
eventboat test examples/linear/tests examples/branching/tests

# run (durable: SQLite store under ./data; or --ephemeral for local dev)
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral
```

A pipeline is three sections joined by `from`; the plugin name is the key:

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

Contract tests (gate 2, §3.2 format):

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

See [examples/](examples/) for the three shipped pipelines (linear,
CEL branching, fan-in).

## POC scope (M1) and recorded trims

Implemented: three-section config with strict whitelists and `${VAR}` /
`${?VAR}` substitution; CEL predicate host (zero custom functions, error =
not-passed + counter); Starlark host (precompiled programs, lazy + COW
payload/meta bindings, `json`/`math` whitelist, 100k step budget, backtraces
into dead letters); engine with spool/settle/checkpoint/backpressure/replay
on SQLite (`modernc.org/sqlite`, pure Go — no hand-written WAL); dead letter
store; JSON-Schema-enforced plugin registry (sources: kafka, http_server,
cron, file; sinks: kafka, http, file, drop; codecs: json, raw); CLI
`verify` / `test` / `run` with `--json`.

Trimmed or decided beyond the spec (recorded per redesign-v3-review.md):

- **Trimmed:** `repl` / `plugin` CLI commands, the conformance corpus and the
  full benchmark suite of §7.4 M1 (minimal Go benchmarks exist:
  CEL predicate ≈ 290 ns/op, simple Starlark transform ≈ 1.4 µs/op on the
  dev machine). `explain`, `replay`, job pipelines (`run`/`parameters`),
  MCP server and observability stack are M2+ (redesign-v3.md §7.4).
- **Deployment-level config** (open question #10) is CLI flags for now:
  `--data-dir`, `--ephemeral`.
- **Modules:** the load whitelist is `json` + `math`; there is no loadable
  `strings` module in go-starlark — string methods are built into the string
  type (review R3).
- **Transform failures** retry on the incoming edge's delivery policy, then
  dead letter (review R6). A fan-out with zero matching edges settles the
  message as filtered and counts it (review R7). `split` turns a JSON array
  payload into one message per element; children share the parent
  message_id (review R8).
- **Spool stores raw bytes + codec marker** (review R9). Sources resume from
  their settled watermark after a crash; the unsettled tail may be re-emitted
  in addition to the spool replay — duplicate delivery, never loss.
- **`order_key`** is evaluated at sinks into the message key (e.g. Kafka
  partition key); full per-key ordered sharding is P1. `workers` gives
  transforms per-node concurrency.
- **Plugin names colliding with framework fields** are rejected at
  registration (review R5).

## Repository layout

```
cmd/eventboat/        CLI: verify / test / run
internal/config/      typed config, strict loader, env+constants substitution
internal/ir/          static IR: DAG, compiled CEL/Starlark, topology checks, lint
internal/lang/        celhost (predicates), starhost (Starlark sandbox host)
internal/engine/      spool admission, DAG execution, settle, delivery, DLQ
internal/store/       SQLite + in-memory spool/checkpoint/dead-letter stores
internal/registry/    plugin registration with mandatory JSON Schemas
internal/registry/builtin/  kafka/http_server/cron/file sources, kafka/http/file/drop sinks, json/raw codecs
internal/testkit/     injection/capture/fault-injection primitives
internal/testrun/     §3.2 contract-test runner
examples/             linear, branching (CEL), fan-in pipelines + suites
legacy/               archived v2 implementation (not imported, not modified)
```

## Development

```bash
go build ./...
go test ./...          # includes the seven TestInvariant_* reliability tests
go test -race ./...
```

Design documents: [redesign-v3.md](redesign-v3.md) (the v3 spec — the single
source of truth), [redesign-v3-review.md](redesign-v3-review.md)
(pre-implementation review). Historical: [riverpod-design.md](riverpod-design.md),
[competitor-research.md](competitor-research.md), [review-2026-08.md](review-2026-08.md),
[design-review.md](design-review.md).
