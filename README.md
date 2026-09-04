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

> **Status: v3 POC** (milestones M1 + M2 + M3 of [redesign-v3.md](redesign-v3.md)).
> The v2 implementation is archived under [legacy/](legacy/) and is not
> compatible. Pre-implementation design reviews: [redesign-v3-review.md](redesign-v3-review.md)
> (M1, verdict: pass, 13 findings), [redesign-v3-review-m2.md](redesign-v3-review-m2.md)
> (M2, verdict: pass, no blockers) and [redesign-v3-review-m3.md](redesign-v3-review-m3.md)
> (M3, verdict: pass, no blockers). License: Apache-2.0.
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

# gate 4: the agent operations surface (§3.4)
eventboat mcp --stdio                       # MCP tools over stdio (agent hosts spawn this)
eventboat mcp --http                        # MCP over HTTP + Admin REST + SSE + read-only UI
eventboat run --config-dir pipelines/       # multi-pipeline daemon with admin on :7788

# the extension ladder (M3, §4.5/§6.5): plugins, WASM, CESQL
eventboat [--json] plugin catalog           # registered plugins with ABI versions
# external gRPC source/sink in any language: docs/plugins.md (proto + handshake + manifest)
# WASM transform tier (wazero, capability sandbox): docs/wasm.md
# CESQL edge dialect: when: { lang: cesql, expr: "region = 'EU' AND data.amount > 100" }

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

## POC scope (M1 + M2 + M3) and recorded trims

Implemented: three-section config with strict whitelists and `${VAR}` /
`${?VAR}` / `${constants.x}` / `${parameters.x}` substitution; CEL predicate
host (zero custom functions, error = not-passed + counter); Starlark host
(precompiled programs, lazy + COW payload/meta bindings, `json`/`math`
whitelist, 100k step budget, backtraces into dead letters); **the extension
ladder (M3)**: out-of-process **gRPC source/sink plugins** (protocol
`eventboat.plugin.v1`, stdout JSON handshake + static manifest so verify
never spawns processes, per-node `version:` pins checked at verify and
handshake, third-party example + acceptance in `examples/plugins/` and
[docs/plugins.md](docs/plugins.md)), **WASM transforms** under wazero
(capability sandbox, per-invoke wall-clock + memory budgets, `wasm:` main
field, guests buildable with the standard Go toolchain,
[docs/wasm.md](docs/wasm.md)) and the **CESQL edge dialect**
(`when: {lang: cesql}` reusing the official CloudEvents parser, the official
TCK vendored and 100% passing in CI, `data.*` payload extension); engine with
spool/settle/checkpoint/backpressure/replay on SQLite (`modernc.org/sqlite`,
pure Go — no hand-written WAL); dead letter store; **job pipelines**
(`run`/`parameters`/`hooks`/`catchup_window`/`overlap` + sql source with
keyset pagination and watermarks + run history + `trigger`/`jobs` CLI);
**explain** (symbolic + message-level with script dry-run) and **replay**
(dead letters / spool windows / job runs); **MCP server** (official Go SDK:
stdio + Streamable HTTP, 14 tools), **Admin REST + SSE + embedded read-only
UI**, Runtime deployment config; **OpenTelemetry** (Prometheus + OTLP dual
export, 27 metrics, job-run spans); JSON-Schema-enforced plugin registry with
ABI versions; CLI `verify`/`test`/`run`/`trigger`/`jobs`/`explain`/`replay`/
`plugin`/`mcp` with `--json`.

Trimmed or decided beyond the spec (recorded per redesign-v3-review.md):

### Decision ledger — M1 review round-2 fixes (D1–D4)

The four rulings from the M1 fix round 2 (commit 879adfc):

- **D1 — single-pass substitution**: values injected for environment
  variables are never re-scanned for `${...}` (no injection-style second
  expansion). Nested expansion, if ever wanted, is a new feature requiring
  its own ruling.
- **D2 — diagnostics quote the offending reference verbatim**, including
  the optional marker `?`, so the exact occurrence is locatable.
- **D3 — test directory mode treats unreadable/unparseable YAML as a hard
  error**: a suite file that cannot even be probed must not silently
  vanish behind the skipped count.
- **D4 — `test` exit codes**: a mixed directory containing broken files
  exits 1 (the valid suites still run); a directory with no valid suites
  but broken files exits 2.

### POC design decisions (review R6–R9)

The four behavioral gap-decisions the pre-implementation review required
(redesign-v3-review.md R6–R9):

- **transform failure retries on the incoming edge** (`when` delivery
  policy of the edge that delivered the message to the failing node; fan-in
  takes the strictest policy), then dead letters with the Starlark backtrace
  (review R6).
- **fan-out with zero matching edges settles as filtered**: it is a
  normal outcome of conditional routing, counted in
  `eventboat_fanout_no_match_total`; it never dead letters silently (review R7).
- **`split` turns a JSON array payload into one message per element**;
  children share the parent's spool identity and `message_id`, and the parent
  settles only when all children settle (review R8).
- **the spool stores raw bytes + codec marker**, not the decoded form:
  replay stays compatible with codec upgrades (review R9; spec open question
  #6). Sources resume from their settled watermark after a crash; the
  unsettled tail may be re-emitted on top of the spool replay — duplicate
  delivery, never loss.

Other recorded trims:

- **Trimmed (still open):** `repl` CLI, the conformance
  corpus and the full benchmark suite of §7.4 M1 (minimal Go benchmarks
  exist: CEL predicate ≈ 290 ns/op, simple Starlark transform ≈ 1.4 µs/op
  on the dev machine, plus the M3 Starlark-vs-WASM benchmark below). From
  §5.10: `telemetry`/`dlq`/`codecs` pipeline
  sections remain unimplemented (telemetry endpoints live in the Runtime
  config; dead-letter retention and named codec reuse are P2). Per-message
  span sampling and tail masking rules are P2 (review R12/R16). A
  kafka-broker integration test (testcontainers) is a CI stretch, not on
  the critical path. M3 trims: no automatic restart of crashed gRPC plugin
  processes (fail fast; redeploy recovers), no external codec plugins
  (decode/encode stays built-in), no TLS on the plugin loopback connection
  (one-shot token instead), WASM guests see only the payload (no metadata
  channel — chain a `script:` node to touch meta), and the CI benchmark
  job is informational (`continue-on-error`), not a regression gate.
- **`limits` section** (M2): `max_in_flight` maps to the engine's spool
  admission high watermark and `drain_timeout` bounds graceful shutdown
  (replacing a hardcoded 10s); a `workers` total quota is trimmed (P2).
- **`lint_constant_unused`** counts `${constants.x}` references on the
  loader's pre-substitution record, so substituted references no longer read
  as unused.
- **Deployment-level config** (open question #10): `kind: Runtime` YAML
  (storage/admin/mcp/telemetry) resolved from `--runtime`, then
  `./eventboat.yaml`, then defaults; `--data-dir`/`--ephemeral` remain as
  overrides.

### The operations surface (M2, §3.4) — rulings

- **MCP SDK**: the official Go SDK (`github.com/modelcontextprotocol/go-sdk`,
  stable v1.x — M2 review §一). mark3labs/mcp-go was rejected (v0.x).
  `eventboat mcp --stdio` speaks MCP for agent hosts; `--http` serves
  Streamable HTTP at `/mcp` next to the Admin REST.
- **One implementation**: `internal/ops` is the whole tool surface; the MCP
  tools and the Admin REST endpoints are thin shells over it — REST, MCP and
  `--json` CLI outputs share the same Go structs and JSON shapes. OpenAPI
  single-source generation is deferred to M4 (allowed); shape consistency is
  enforced by the shared structs.
- **Tool naming**: the MCP tools are `dlq_query`/`dlq_replay` per spec §3.4
  (the `dlq` abbreviation is retained industry-wide per spec v1.6, matching
  `replay --dlq` and the `dlq` config section). Formerly named
  `dead_letter_query`/`dead_letter_replay` in the first M2 cut (renamed
  after audit — kept here for old-reference retrieval). The Admin REST
  endpoint is `GET /admin/dlq/{pipeline}`. All §3.4 tools are implemented:
  catalog, verify, test, explain, deploy, status, jobs, trigger, tail,
  dlq_query, dlq_replay, drain, pause, resume.
- **verify reports, deploy enforces**: the `verify` tool returns structured
  diagnostics with an `ok` verdict (a successful tool call — agents iterate
  on diagnostics); `deploy` FAILS the tool call when verification fails.
  There is no bypass around verify-first.
- **deploy** persists the submitted config under `<data-dir>/pipelines/`
  (job managers reload it per run), drains the previous instance, then
  starts the replacement.
- **pause/resume/drain** stop the pipeline instance; sources resume from
  their persisted commit states (at-least-once covers the window).
- **tail** samples recent deliveries per node (bounded ring of 100, payloads
  truncated at 512 bytes); masking rules are P2 (review R12).
- **The agent-loop acceptance** (§7.4 M2 core): `TestAgentLoopOverMCP` drives
  the real binary over MCP stdio — catalog → verify(bad rejected, good
  accepted) → test → explain → deploy → status → parameterized trigger →
  jobs → broken-script deploy rejected → fixed redeploy → watermark-resumed
  incremental run → tail.
- **Runtime config** (open question #10, review R13): `kind: Runtime` YAML
  (storage/admin/mcp/telemetry), resolved from `--runtime`, then
  `./eventboat.yaml`, then defaults; CLI flags override. The admin listener
  binds 127.0.0.1 by default and has NO authentication (POC boundary —
  recorded).

### Observability (M2, §6.6)

- **Dual export via the OTel Go SDK**: one MeterProvider with two readers —
  Prometheus exposition at `/metrics` (daemon surface) and OTLP/HTTP push
  when `telemetry.otlp_endpoint` is configured. Fully disabled telemetry
  costs nothing (noop providers). Single-pipeline `run` gets OTLP only (no
  HTTP surface); `run --config-dir` / `mcp --http` serve both.
- **25 metrics, `eventboat_` prefix** — exactly the review's list:
  messages_in_total, messages_settled_total, dead_letter_total (with
  reason_class), dlq_write_failures_total, cel_eval_errors_total,
  fanout_no_match_total, delivery_retries_total, optional_drops_total,
  decode_errors_total, spool_failures_total, backpressure_events_total,
  script_step_budget_exhausted_total, jobs_started_total,
  jobs_overlap_skipped_total, jobs_catchup_skipped_total,
  jobs_completed_total (by status), job_rows_read_total,
  job_rows_delivered_total; histograms script_duration_seconds,
  sink_write_duration_seconds, job_duration_seconds,
  settle_latency_seconds; gauges in_flight_messages, spool_depth,
  pipeline_paused.
- **Spans** (review R16: no per-message spans at routing rates): one span
  per job run (`eventboat.job.run`, attributes pipeline/run_id/trigger,
  terminal status + recorded errors); deploy/job events also flow through
  the SSE stream. Script backtraces ride the dead-letter records and span
  error events; per-message span sampling is P2.
- The M1 atomic counters remain the engine's internal bookkeeping and the
  `--json` CLI/status source; OTel instruments record the same events
  (`SettledCount` split from the checkpoint pointer, review R5).

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

### The extension ladder (M3, §4.5/§6.5) — rulings

From [redesign-v3-review-m3.md](redesign-v3-review-m3.md) (all verified
against live dependencies, several empirically) plus implementation-time
discoveries:

- **gRPC plugins (§6.5)**: protocol `eventboat.plugin.v1`
  ([proto/eventboat/plugin/v1/plugin.proto](proto/eventboat/plugin/v1/plugin.proto)).
  A plugin is any program that listens on 127.0.0.1, prints one JSON
  handshake line to stdout and serves gRPC; the host dials with the
  handshake's one-shot token as `eventboat-auth` metadata. Lifecycle,
  backpressure (gRPC flow control carries the admission gate), batch
  semantics and the pull/exhaustion convention are documented in
  [docs/plugins.md](docs/plugins.md) — the acceptance test builds the
  third-party-style example (`examples/plugins/ticker-source`, a separate Go
  module importing only the generated protocol code) exactly the way an
  outside implementer would and runs it through verify and a real engine run.
- **Verify stays side-effect free**: external plugins ship a static
  **manifest** (kind/name/version/capabilities/JSON schema); gate 1
  validates the plugin block against the manifest file, never a spawned
  process. At run time the handshake is cross-checked against the manifest
  (name/version/kind/capabilities) — drift fails loudly. A `grpc:` block on
  a compiled-in plugin name is a verify error.
- **Config shape**: `grpc: { command, env, schema }` and the optional
  `version:` pin are node-level framework fields (next to `decoder`/
  `encoder`). A version mismatch with the registry (builtins), the manifest
  (external) or the handshake (runtime) is an error — the §6.5 rule that
  makes "documented but absent" impossible. `eventboat plugin catalog`
  lists everything with versions.
- **Generated protocol code lives in `pkg/pluginv1`, not `internal/`**:
  Go's internal-package visibility would make it unimportable from the
  separate-module third-party example — the exact audience the protocol
  exists for.
- **Plugin Init carries the config**: the engine only calls a source's
  `Init` when persisted state exists (M1 semantics — discovered while
  debugging the acceptance test), and `registry.Sink` has no Init at all;
  the rpcplugin adapters therefore self-init (config delivery) before their
  first stream/Write. Documented in docs/plugins.md.
- **WASM tier (§4.5 tier 3)**: wazero v1.12.0, wasip1 **reactor** modules
  (`_initialize`). The decisive toolchain finding: **the standard Go
  toolchain builds guests** (`GOOS=wasip1 go build -buildmode=c-shared`) —
  no TinyGo, no Rust, no extra CI toolchain; the guest source lives under
  `testdata/` (ignored by the normal build), its compiled `.wasm` is
  committed so `go test ./...` needs nothing extra, and CI rebuilds the
  guest from source for the wasm tests. Guests may be written in any
  wasip1 reactor language (ABI in [docs/wasm.md](docs/wasm.md)).
- **WASM resource model**: wazero has no instruction metering, so budgets
  are per-invoke wall-clock (`timeout_ms`, opt-in) + a memory pages cap
  (always on). Enforcing the deadline requires wazero's context-close
  mechanism, **measured at ~5x throughput cost on loop-heavy guests** (a
  manual watchdog-close was tested and deadlocks — there is no cheaper kill
  path in wazero 1.x). **M3-audit J2 ruling: fast mode (no kill switch) is
  the default** — a tier whose only reason to exist is performance cannot
  default to slower than Starlark; the runaway-guest risk is availability
  (one wedged worker, backpressure stall, no data loss, restart clears it),
  not correctness. Guardrails: `wasm_no_kill_switch` verify warning
  (`--strict` escalates) on unset `timeout_ms`, and a zero-interference
  slow-call watchdog (engine default 5s, `WasmSlowCallWarnMs`) logs a wedged
  invoke once. Traps/timeouts in protected mode kill the instance, which is
  transparently re-instantiated per worker.
- **WASM wire format**: payload as JSON bytes in/out (what a Starlark
  script would see/produce); metadata passes through untouched; guest
  errors/traps/bad JSON follow the same delivery→dead-letter path as
  script failures. `explain` does not dry-run guests (documented).
- **CESQL (§4.7)**: `when: { lang: cesql, expr: ... }` reuses the official
  `cloudevents/sdk-go/sql/v2` parser — zero self-written grammar. The
  official TCK is vendored (275 cases, **100% passing in CI**) from the
  SDK's own v2.16.2 tag; two files have drifted in the spec repo's main
  branch since that release (not_operator, context_attributes_access), so
  the acceptance rule is pinned to the vendored copy — upgrading the parser
  means re-vendoring and re-reviewing.
- **CESQL dialect constraints (honesty notes)**: CESQL identifiers are
  alphanumeric only (CloudEvents attribute shape) — meta keys with
  underscores (`kafka_offset`, `message_id`...) are unreachable in this
  dialect; use CEL for those. The `data.*` payload extension is implemented
  as a string-literal-aware rewrite to synthetic camelCase identifiers
  (`data.amount` → `dataAmount`) because the CESQL lexer has no dot token
  and no underscore — identifiers starting with "data" are reserved for it.
  Extension mode is "CESQL subset + documented extension" per spec open
  question #5, and never advertised as compatible.
- **wazero maintenance** has slowed upstream (core maintainer moved on;
  some downstreams migrating off it). Accepted for M3: pinned version, pure
  Go, zero dependencies, and we only use the conservative surface (reactor
  instantiation, memory imports, context close). Revisit before betting
  further on the tier.
- **M2-review-#6 debt cleared**: job pipelines with multiple pull sources
  now warn at verify (`job_multiple_pull_sources`): the `cursor` parameter
  binds the first pull source's watermark.

### Performance (M3 benchmark: Starlark vs WASM)

Reproduce: `go test ./internal/wasmhost/ -bench BenchmarkHeavyTransform -benchtime 2s`
(same multi-pass aggregation over a 2000-element array per message, Starlark
script vs the Go WASM guest; the CI `bench` job runs it on every push as an
informational artifact, not a gate):

| variant | per message | allocations |
|---|---|---|
| Starlark heavy script | 5.9 ms | 122,092 |
| WASM, fast mode (default) | 2.5 ms | 4 |
| WASM, protected (`timeout_ms > 0`) | 13.1 ms | 19 |
| Starlark light script | 22 µs | 2,023 |
| WASM light | 103 µs | 15 |

(Windows/amd64, i5-14600KF; Linux numbers via the CI job.) Reading: heavy
per-message computation is where the WASM tier pays (2.3x and ~30,000x fewer
allocations in the default fast mode); the kill switch costs ~5x, so — per
the M3-audit J2 ruling — **fast mode is the default** (verify warns on an
unset `timeout_ms`; a slow-call watchdog logs wedged invocations once), and
protection is opt-in. For light mapping Starlark wins outright — the
ladder's trigger standard (§4.6: performance or dependencies, never
"complex logic") is quantified, not vibes.

## Repository layout

```
cmd/eventboat/        CLI: verify / test / run / trigger / jobs / explain / replay / plugin / mcp
proto/                 eventboat.plugin.v1 — the out-of-process plugin protocol (.proto source)
pkg/pluginv1/          generated protocol stubs (importable by third-party plugins)
docs/                  plugins.md (gRPC SDK), wasm.md (guest ABI and tier guide)
internal/config/      typed config, strict loader, env+constants+parameters substitution
internal/ir/          static IR: DAG, compiled CEL/Starlark/CESQL, topology checks, job semantics, lint
internal/lang/        celhost (predicates), cesqlhost (CESQL dialect + official TCK), starhost (Starlark sandbox host)
internal/wasmhost/    wazero host: capability sandbox, per-invoke budgets, guest under testdata/
internal/rpcplugin/   gRPC plugin host: process spawn/handshake, source/sink adapters
internal/engine/      spool admission, DAG execution, settle, delivery, DLQ, pull sources
internal/jobs/        job runtime: scheduler, catchup, overlap, run lifecycle, hooks
internal/store/       SQLite + in-memory spool/checkpoint/dead-letter/job-history stores
internal/registry/    plugin registration with mandatory JSON Schemas + ABI versions
internal/registry/builtin/  kafka/http_server/cron/file/sql sources, kafka/http/file/drop sinks, json/raw codecs
internal/explain/     deterministic walkthroughs + topology rendering
internal/ops/         the operations service behind MCP and Admin REST
internal/mcpserver/   MCP tools (official Go SDK)
internal/admin/       Admin REST + SSE + embedded read-only UI
internal/runtimecfg/  kind: Runtime deployment configuration
internal/obs/         OpenTelemetry: dual export, 27 metrics, spans
internal/testkit/     injection/capture/fault-injection primitives + fakepull test source
internal/testrun/     §3.2 contract-test runner
examples/             linear, branching (CEL), fan-in, job-sync (sql), plugins/ (third-party-style gRPC source)
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
