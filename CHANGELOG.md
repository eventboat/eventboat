# Changelog

All notable changes to Eventboat. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver
(pre-1.0: the API surface may still shift between minor versions).

## Unreleased

### Changed

- **Transforms are now registered plugins, peers of sources and sinks**
  (spec v1.19). `registry` gains `KindTransform`, a `Transform` interface
  (`Init(TransformEnv)` / `Apply(*Message) ([]*Message, error)` / `Close`)
  with two optional extensions (`TransformCloner` — the engine clones once
  per worker goroutine, used by wasm whose module instances are not
  goroutine-safe; `TransformFlavor` — feeds the script/wasm metrics), plus
  `RegisterTransform` (hand-written schema escape hatch) and
  `RegisterTransformT` (typed; supports scalar root configs — the script
  plugin's config is the Starlark source text itself, so transform plugin
  blocks are not forced to mappings). The builtins script/split/wasm
  register through it like every other plugin; existing pipeline YAML is
  unchanged (`script: |`, `split: {}`, `wasm: {...}` keep working), the
  plugin-name-as-key convention now applies to `transforms:` nodes, and
  `version:` pins work there too. A plugin transform returning zero outputs
  filters the message (settled + NoMatch counter, the same semantics as an
  edge predicate with no matching edge). `plugin catalog` / `plugin schema`,
  the MCP catalog, LSP completion/hover and the schema goldens expose the
  new kind; third-party compile-in transforms register through the same
  API (out-of-process gRPC transform plugins remain future work). This
  supersedes the v1.17 ruling that kept hand-parsed transforms for
  line-precise diagnostics:
  - Diagnostic codes retired: `cfg_transform_main_field` (now
    `cfg_missing_plugin` / `cfg_multiple_plugins`), `cfg_script_type`,
    `cfg_split_type`, `cfg_wasm_type/module/range/allow` (now `plugin_schema`
    issues, same as every other plugin kind).
  - Preserved via error classification: `expr_starlark_compile` (with
    backtrace), `expr_wasm_compile`, `wasm_no_kill_switch`.
  - Script dead-letter backtraces now carry positions as `script:L:C`
    instead of `transforms.<node>.script:L:C` (plugin factories don't know
    node names; the dead-letter record already names the node).
  - `engine.Metrics.TransformRuns` now counts split runs too.

- **Vocabulary rename: Settled → Commit, everywhere.** The settle term was
  unusual for stream processing and forced a translation step at every use
  ("commit offsets HERE" was already how the docs explained it). The source
  contract is now `registry.Source.Commit(ctx, throughSrcSeq)` — called, as
  before, when the engine's contiguous committed frontier advances; sources
  commit their offsets there. Semantics are unchanged: "committed" still
  means a message reached a terminal state (sink ack, dead letter, filtered,
  optional drop), so committing past a dead letter is by design (at-least-once,
  never loss). Renamed across the whole stack: the gRPC plugin ABI
  (`rpc Settled` → `rpc Commit`, `SettledRequest/SettledResponse` →
  `CommitRequest/CommitResponse` — external plugins must be rebuilt),
  engine internals (`settleTracker` → `commitTracker`, `WaitSettled` →
  `WaitCommit`, `SettleSnapshot` → `CommitSnapshot`, `SettledCount` →
  `CommittedCount`), observability (counters
  `eventboat_messages_settled_total` → `eventboat_messages_committed_total`,
  histogram `eventboat_settle_latency_seconds` →
  `eventboat_commit_latency_seconds`, span terminal state `settled` →
  `committed`, ops status field `settled` → `committed`), the run settle
  report keys (`settled_through`/`settled` → `committed_through`/`committed`),
  the job lifecycle status `settling` → `committing` (runs persisted with the
  old `settling` status are no longer seen as active after the upgrade —
  re-trigger them), and the throughput benchmark (`BenchmarkSettleThroughput`
  → `BenchmarkCommitThroughput`, bench gate updated). Pre-1.0, no compat
  shims; dated review documents keep the historical wording.

- **Builtin plugin configs are now defined by typed structs.** Each plugin
  declares one config struct (`json` tags name keys, `schema` tags declare
  constraints and defaults) and registers through
  `registry.RegisterSourceT/RegisterSinkT/RegisterCodecT`: the JSON Schema
  is generated from the struct, the factory receives a decoded,
  defaults-applied value instead of `map[string]any`, and the same
  `default` tag drives both the schema annotation and runtime default
  injection (nested structs and `[]struct` included). This removes the
  drift surface where every plugin hand-maintained a schema string plus
  manual type assertions with a second copy of its defaults. The
  validation pipeline, `plugin_schema` diagnostics and external gRPC
  manifest handling are unchanged; `plugin catalog`/`plugin schema`
  output is semantically identical (re-formatted). Generated schemas are
  pinned by goldens in `internal/registry/builtin/testdata/schemas/`
  (regenerate with `-update-schemas`). The string-based `Register*` API
  remains available as an escape hatch. Transforms (script/split/wasm)
  keep their line-precise hand parsing.

### Added

- **Container image, published to GHCR as
  `ghcr.io/eventboat/eventboat`.** A multi-stage `Dockerfile` builds the
  CLI with `CGO_ENABLED=0` (every driver in go.mod is pure Go, SQLite via
  modernc) onto distroless/static as the nonroot user, with `/pipelines`
  and `/data` as the conventional mount points matching
  examples/k8s/deployment.yaml; a bare `docker run` prints the help
  screen and exits 0. The Docker workflow builds linux/amd64 +
  linux/arm64 and pushes on every push to main (`:main`, `:sha-<short>`)
  and on `v*` tags (semver `{{version}}`; no moving `0.x` tag while
  pre-1.0); PRs build without pushing. The k8s example now references
  the published image and pins the nonroot securityContext
  (`runAsNonRoot` + `fsGroup: 65532`).

### Fixed

- **SIGTERM now triggers graceful shutdown** in every long-running verb
  (`run` single-pipeline and `--config-dir`, `mcp`, `replay`,
  `trigger`). Previously only SIGINT was registered, so `docker stop`
  and Kubernetes pod termination killed the process mid-flight without
  the final settle report (at-least-once still held via checkpoint
  recovery, but the clean drain was skipped). `lsp` already handled
  both signals; the others now match it. Verified against the published
  image shape: `docker stop` now logs the settle status line and exits
  0.

## v0.2.0-rc1 (2026-09-05)

First release candidate: all four design milestones (M1–M4) plus the beta
hardening cycle, post-beta cleanup, and the CLI framework migration —
every change reviewed and audited.

### Changed

- **CLI dispatch migrated to `github.com/lynx-go/commands`** (the
  project's own zero-dependency verb-dispatch framework; its two v0.2
  features — global valueless root bool flags and verb-declared usage
  errors — were added for exactly this migration and are proven by it).
  Command names, flags and exit codes are unchanged; the deliberate
  help-surface changes: bare `eventboat` prints the help screen to stdout
  and exits 0 (was: usage on stderr, exit 2); `eventboat help <verb>` and
  verb-level `-h` print per-verb usage and flag defaults (exit 0); usage
  failures (unknown verb/subcommand, flag parse errors, missing required
  flags) exit 2 with a `usage:` hint line on stderr; `--json` remains a
  global flag and is additionally accepted after the verb. Help output is
  pinned by golden snapshots (cmd/eventboat/testdata/help; regenerate with
  `go test ./cmd/eventboat -run TestHelpSnapshots -update`). The cmdX
  executors and their direct unit tests are untouched, and the stdio
  protocol channels stay clean (LSP protocol test and MCP agent-loop test
  green over the migrated binary).

### Removed

- **The archived v2 tree (`legacy/`) is gone from the worktree.** Nothing
  imported it (it was a separate Go module), and the v0.1.0-beta tag
  preserves the full archive, so v2 remains recoverable via git history.
  No behavior change.
- **The `convert` command and `internal/convert` package are removed.**
  There are no production v2 pipelines to migrate, so the migration tool
  (shipped in M4) is retired per the v1.9 "on-demand" ruling. Spec §7.3
  records the tombstone; the §4.8/§7.2 mapping tables remain as a manual
  reference. No other behavior change.

## v0.1.0-beta (2026-09-04)

First beta: the v3 POC milestones (M1–M4) hardened — debt cleared, tests
deepened, naming prerequisites researched. **No new product surface beyond
the listed knobs.** Upgrade/compatibility notes are marked **⚠**.

### Reliability & performance hardening

- **Settle persistence moved off the tracker lock.** Checkpoint/source-state
  writes used to run while holding the settle tracker mutex (every other
  settle, arrival and status poll queued behind each fsync). Advances now
  compute under the lock and flush on the settling goroutine outside it,
  with monotonic guards keeping checkpoint, watermarks and metrics from ever
  regressing. Semantics are unchanged (the seven invariant tests passed
  zero-modification); observers (`WaitSettled`, quiescence) now wait on an
  explicit durability barrier, and a permanently failing store no longer
  wedges quiescence detection. Reference numbers in
  [redesign-v3-review-beta.md](redesign-v3-review-beta.md).
- **Precise dirty tracking in the Starlark host.** Reading through nested
  containers no longer marks the payload dirty, so read-only scripts skip
  the whole-tree write-back (~23% faster on a nested read-only script
  benchmark). Map-tree writes are tracked precisely; trees containing lists
  stay conservatively dirty (native Starlark list mutators cannot be
  intercepted — documented boundary).
- **Pipeline-aggregated backpressure.** Under `run.overlap: all`,
  `limits.max_in_flight` is now the TOTAL across concurrent runs (previously
  per-run: N runs could hold N × max_in_flight). Single-run behavior is
  unchanged.
- `TestJobKill9ResumeFromWatermark` hardened against machine load (the M4
  flake observation): same assertions, 3× budget headroom, CI-scalable via
  `EVENTBOAT_TEST_TIMEOUT_FACTOR`.

### Observability & plugins

- **`telemetry:` pipeline section** (§5.10) landed with two fields:
  - `redact`: glob field paths (`payload.user.email`, `payload.card*`,
    `payload.items.*.sku`). Matched values are masked (`"***"`) in **tail
    entries only** — the spool, deliveries and dead letters are the data
    path and are never altered; non-JSON payloads pass through unmasked.
    Bad patterns are verify errors (`telemetry_redact_pattern`).
  - `span_sample_rate`: per-message spans (`eventboat.message`,
    accept → settled/dead_letter) when set above 0 (default 0 = no spans,
    zero cost — per-message tracing stays opt-in per review R16).
- **gRPC plugin crash policy** (§6.5, M3 trim closed): `grpc.restart:
  fast-fail` (default — unchanged M3 semantics) or `restart` — the host
  respawns a crashed/wedged plugin with exponential backoff (250ms → 30s),
  re-delivers config and the latest Settled state, reconnects source
  streams and retries sink writes once per call. New metric
  `eventboat_plugin_restarts_total{plugin}`. See
  [docs/plugins.md](docs/plugins.md).
- **⚠ new metric**: `eventboat_plugin_restarts_total` (28 `eventboat_*`
  instruments total).

### Testing & CI

- **Kafka integration against a real broker** (testcontainers, KRaft) in a
  dedicated CI job: produce/consume roundtrip through the engine,
  malformed-record dead-lettering, consumer-group rebalance with no double
  delivery.
- **Soak workflow** (nightly + manual dispatch): mixed pipelines under load
  with injected spool/DLQ faults; asserts exactly-once-per-injection settle,
  full checkpoint advance and no goroutine leaks.
- **Performance regression gate**: the CI bench job moved from
  informational to loose thresholds (order-of-magnitude guard) over the CEL
  predicate, Starlark scripts and settle throughput; WASM stays
  informational.
- **golangci-lint v2 baseline** with zero findings (exclusions: generated
  plugin stubs, vendored CESQL TCK, staticcheck QF quickfix class).
- Full suite green under `-race` and `-count=5`.

### Naming & release

- §8.4 naming prerequisites executed (research only):
  [docs/naming-checklist.md](docs/naming-checklist.md) — software space
  still clean, npm/crates/PyPI unregistered, eventboat.io/.dev/.sh without
  DNS delegation, trademark routes and fees surveyed, three-model
  zero-prior questionnaire ready. Domain registration, trademark filing and
  the remaining model checks are listed as user actions (irreversible steps
  stay with the user).

## Earlier milestones (POC, unreleased tags)

- **M1** (2026-09): three-section config + strict loader, CEL/Starlark
  hosts, engine with spool/settle/checkpoint/backpressure/replay on SQLite,
  the seven reliability invariants, CLI `verify`/`test`/`run`.
- **M2** (2026-09): job pipelines (sql pull source, scheduler, catchup,
  overlap, run history), explain/replay, MCP server (14 tools) + Admin
  REST/SSE/UI, OpenTelemetry (dual export), Runtime config.
- **M3** (2026-09): extension ladder — out-of-process gRPC plugins,
  WASM transforms (wazero), CESQL edge dialect (official TCK 100%).
- **M4** (2026-09): convert (v2 → v3), LSP, csv/avro/protobuf codecs,
  `plugin schema` export, `repl`.
