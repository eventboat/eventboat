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

> **Status: v3 POC** (milestones M1 + M2 of [redesign-v3.md](redesign-v3.md)).
> The v2 implementation is archived under [legacy/](legacy/) and is not
> compatible. Pre-implementation design reviews: [redesign-v3-review.md](redesign-v3-review.md)
> (M1, verdict: pass, 13 findings) and [redesign-v3-review-m2.md](redesign-v3-review-m2.md)
> (M2, verdict: pass, no blockers). License: Apache-2.0.
> 中文说明见 [README_ZH.md](README_ZH.md).

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

# gate 2: contract tests — in-process real engine, fixture injection, capture.
# Directory mode recurses; YAML files without a top-level `suite:` key
# (pipelines etc.) are skipped and counted.
eventboat test examples

# run (durable: SQLite store under ./data; or --ephemeral for local dev)
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral

# job pipelines (§5.8): scheduled/triggered runs with run history
eventboat run --config examples/job-sync/pipeline.yaml              # scheduler + catchup
eventboat trigger --config examples/job-sync/pipeline.yaml \
  --parameters '{"from":"2026-09-01T00:00:00Z","to":"2026-09-02T00:00:00Z"}'   # backfill
eventboat jobs list --config examples/job-sync/pipeline.yaml        # history
eventboat jobs show <run-id> --config examples/job-sync/pipeline.yaml

# gate 3: explain (deterministic walkthrough) and replay (§3.3)
eventboat explain --config examples/branching/pipeline.yaml                       # symbolic
eventboat explain --config examples/branching/pipeline.yaml --message sample.json # message-level
eventboat explain --config examples/branching/pipeline.yaml --topology            # mermaid + ASCII
eventboat replay --config p.yaml --dlq --since 2h --dry-run                       # preview paths
eventboat replay --config p.yaml --dlq --where 'payload.region == "eu"' --delete  # reinject + prune
eventboat replay --config p.yaml --spool --from 42                                # spool window
eventboat replay --config job.yaml --job <run-id>                                 # "restart failed"

# the example's sqlite source database regenerates with:
go run ./examples/job-sync/seed
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

### Decision ledger (M1)

The four behavioral gap-decisions the pre-implementation review required
(redesign-v3-review.md R6–R9), numbering used in commits:

- **D1 — transform failure retries on the incoming edge** (`when` delivery
  policy of the edge that delivered the message to the failing node; fan-in
  takes the strictest policy), then dead letters with the Starlark backtrace
  (review R6).
- **D2 — fan-out with zero matching edges settles as filtered**: it is a
  normal outcome of conditional routing, counted in
  `eventboat_fanout_no_match_total`; it never dead letters silently (review R7).
- **D3 — `split` turns a JSON array payload into one message per element**;
  children share the parent's spool identity and `message_id`, and the parent
  settles only when all children settle (review R8).
- **D4 — the spool stores raw bytes + codec marker**, not the decoded form:
  replay stays compatible with codec upgrades (review R9; spec open question
  #6). Sources resume from their settled watermark after a crash; the
  unsettled tail may be re-emitted on top of the spool replay — duplicate
  delivery, never loss.

Other recorded trims:

- **Trimmed:** `repl` / `plugin` CLI commands, the conformance corpus and the
  full benchmark suite of §7.4 M1 (minimal Go benchmarks exist:
  CEL predicate ≈ 290 ns/op, simple Starlark transform ≈ 1.4 µs/op on the
  dev machine). MCP server, explain/replay and the observability stack are
  the remaining M2 steps (redesign-v3.md §7.4) — see the M2 review.
- **`limits` section** (M2): `max_in_flight` maps to the engine's spool
  admission high watermark and `drain_timeout` bounds graceful shutdown
  (replacing a hardcoded 10s); a `workers` total quota is trimmed (P2).
- **`lint_constant_unused`** counts `${constants.x}` references on the
  loader's pre-substitution record, so substituted references no longer read
  as unused.
- **Deployment-level config** (open question #10) is CLI flags for now:
  `--data-dir`, `--ephemeral`.

### explain / replay (M2, §3.3) — rulings

- **Scripts dry-run in message-level explain** (review R10): the Starlark
  sandbox is deterministic and side-effect free, so with `--message` the
  walkthrough executes scripts on the sample and evaluates each outgoing
  CEL edge against the TRANSFORMED payload — the same answer production
  would give. A failing script renders its backtrace and the dead-letter
  consequence; without `--message` nothing executes (symbolic summary:
  statement counts, budgets, condition texts).
- **Injection enters INTO the target node**: replaying at a transform
  re-runs its script (the "fix the script, replay the dead letter" flow);
  at a sink it writes under the sink's delivery policy; at a source it
  goes through the full spool path.
- **Replayed messages keep their original `message_id`** (idempotent sinks
  can deduplicate re-deliveries) and are stamped `meta.is_replay=true`
  (plus `original_message_id`).
- **`replay --spool --from N`** walks the spool window PAGEWISE
  (`ReplayPage`, review R7 — the engine's crash recovery uses the same
  path); **`--job <run-id>`** replays that run's dead letters
  (`replay --job` is the dagu "restart failed" equivalent); `--dry-run`
  explains each selected message instead of delivering; `--delete` prunes
  replayed dead letters after successful reinjection.

### Job pipelines (M2, §5.8) — implemented semantics and decisions

- **Lifecycle**: `pending → running → settling → success | partial | failed |
  canceled`, one `job_run` history row per run (run-id, trigger,
  trigger-provided parameters, scheduled_for tick, counts, error) in the
  same SQLite store, pruned by `run.retention.history`.
- **Per-run engines**: each run resolves its parameters and builds its own
  IR/engine over the shared store (`${parameters.x}` substitution happens at
  trigger time, §5.9). `overlap: all` runs engines concurrently with
  per-engine backpressure; `skip` rejects with a counter; `latest` cancels
  the active run.
- **Cancel semantics** (review R2): a canceled run waits at most
  `limits.drain_timeout` for in-flight work, then dead-letters the
  outstanding set with reason `job canceled` — terminal, auditable,
  replayable; the checkpoint prefix never wedges.
- **Crash recovery**: runs found in `pending/running/settling` resume on
  startup: the engine replays the spool beyond the checkpoint (invariant 3)
  while pull sources re-pull after the settled watermark (invariant 7) —
  the kill-9 resume test (`TestJobKill9ResumeFromWatermark`) covers the
  combination.
- **catchup_window** (review §三.2 / open question #9): at most ONE catchup
  run — the latest missed tick inside the window; older missed ticks are
  counted (`eventboat_jobs_catchup_skipped_total`) and skipped. A pipeline
  with no history never catch-runs.
- **skip_if_successful** keys on the tick identity (`scheduled_for`),
  which also makes catchup idempotent across restarts.
- **Hooks** (review R14): `hooks.failure` fires on failed AND partial runs,
  `hooks.success` on success; hook sinks are inline plugin blocks validated
  by their JSON Schema; delivery is best-effort (3 attempts) and carries the
  run summary JSON. Hooks are notifications, not pipeline data.
- **Parameters**: typed declarations validated at verify (self-consistent
  defaults, enum/pattern/min/max) and at trigger time; `${parameters.x}`
  substitutes anywhere in job pipelines only; scripts/predicates read the
  frozen `parameters.x` binding. The `cursor` binding resolves per pull
  source against that source's own settled watermark (no watermark yet →
  empty string); `now` resolves to the run start. Multi-source job
  pipelines are legal but single-source is recommended (script-side `cursor`
  binds the first source's watermark).
- **sql source** (`capabilities: [pull]`): drivers `mysql | postgres |
  sqlite` — all pure Go (go-sql-driver/mysql, jackc/pgx/v5 stdlib,
  modernc.org/sqlite). Named `:arg` bindings are rewritten per dialect
  (string literals, quoted identifiers and PG `::` casts are respected);
  keyset pagination wraps the user query as a derived table comparing the
  key tuple; `cursor.column` is the resume watermark; `emit: row|page` (page
  payloads are arrays for `transform.split`). The sqlite dialect exists so
  examples and CI run without a server; mysql/postgres need no config
  changes beyond `driver`/`dsn`.
- **Job history records the trigger-provided parameters** (operator intent,
  auditable backfills); resumes re-resolve `cursor`/`now` against the
  current watermark rather than re-using stale resolved values.
- **Contract suites on job pipelines** inject at the pull node with the
  real sources disabled (deterministic); the real sql pull path is covered
  by `TestJobTriggerAndHistoryCLI` against the example's sqlite database.
- **Spec erratum found** (recorded against redesign-v3.md §5.8): the spec's
  own example script line `payload.amount_txt = "%.2f" % payload.amount`
  does not run on go-starlark — its `%` interpolation supports neither
  float-precision nor width conversions (`"unknown conversion"`). The
  shipped example rounds via `math.floor` instead.
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
cmd/eventboat/        CLI: verify / test / run / trigger / jobs
internal/config/      typed config, strict loader, env+constants+parameters substitution
internal/ir/          static IR: DAG, compiled CEL/Starlark, topology checks, job semantics, lint
internal/lang/        celhost (predicates), starhost (Starlark sandbox host)
internal/engine/      spool admission, DAG execution, settle, delivery, DLQ, pull sources
internal/jobs/        job runtime: scheduler, catchup, overlap, run lifecycle, hooks
internal/store/       SQLite + in-memory spool/checkpoint/dead-letter/job-history stores
internal/registry/    plugin registration with mandatory JSON Schemas
internal/registry/builtin/  kafka/http_server/cron/file/sql sources, kafka/http/file/drop sinks, json/raw codecs
internal/testkit/     injection/capture/fault-injection primitives + fakepull test source
internal/testrun/     §3.2 contract-test runner
examples/             linear, branching (CEL), fan-in, job-sync (sql + run/parameters)
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
