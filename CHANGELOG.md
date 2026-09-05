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

- **The spool is now bounded by retention** (third performance finding:
  nothing ever deleted spool rows, so SQLite disk and `--ephemeral`
  memory grew linearly with total messages). Once the DURABLE checkpoint
  reaches C, rows at or below `C - spool_retention` are deleted in one
  batched sweep per window of checkpoint progress (piggybacked on the
  checkpoint flush path — never per message; the cutoff derives from the
  persisted position, so a stretch of failing checkpoint writes cannot
  trim into the replay window). New Runtime config knob
  `storage.spool_retention` (rows kept behind the checkpoint; default
  10,000, `0` = default, negative rejected). Crash recovery is
  unaffected — it replays only beyond the checkpoint, always above the
  cutoff — and `replay --spool` keeps the retained window as queryable
  history: replaying older-than-retention seqs now finds nothing (raise
  the knob for deeper backfills; dead letters are a separate table and
  never trimmed). The in-memory store mirrors the bound so `--ephemeral`
  runs stop growing too, and its `ReplayPage` windows the slice instead
  of copying the whole spool per page (replay was O(N²) in total
  pages).
- **The SQLite store now pairs WAL with `synchronous=NORMAL`** (DSN
  pragma; it previously left SQLite's default FULL in place): commits
  stop fsyncing the WAL per write and fsync at WAL checkpoints instead.
  At-least-once semantics are preserved — NORMAL survives a process
  crash intact (the primary failure model), and a power loss can only
  widen the replay window (duplicates on re-emit, never loss, the same
  contract as a checkpoint write that never reached disk). The store
  also opens two connections now (WAL = one writer + concurrent
  readers), so admin/jobs status reads stop convoying behind spool and
  checkpoint writes.

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

- **`explain --message` prints the wasm disclosure line again.** The
  transform-plugin refactor left non-explain-safe transforms silent in
  message traces: a pipeline with a wasm transform showed downstream
  MATCH/no-match output that read as if it were based on the transformed
  payload while it actually evaluated the pre-transform one. The trace now
  prints `transform.wasm (module ..., entrypoint ...) — guest not dry-run;
  downstream sees the pre-transform payload` (third-party transforms
  without the explain-safe capability get the same disclosure); the guest
  is still never executed in explain.
- **A failed transform `Clone()` now fails the pipeline instead of sharing
  one instance across workers.** When a plugin implements
  `TransformCloner` (the contract for instances that are not
  goroutine-safe — wasm module instances) but `Clone()` errors, the engine
  used to log and continue with `workers` goroutines on the shared master:
  a data race on exactly the state the plugin declared unsafe. The failure
  is now worker-fatal — the engine cancels and drains, `Run` returns the
  error (`node "x": transform worker clone: ...`), anything already in
  flight stays uncommitted for replay (at-least-once holds), and job runs
  fail instead of being marked successful. `Apply` never runs on the
  shared instance; plugins that do not implement `TransformCloner` still
  share one Init'ed instance by design (the author opted into sharing).
- **Two hot-path defects that degraded long runs** (performance review):
  - The commit tracker's per-source bookkeeping (`srcTracker.arrivedAt` /
    `committedAt`) was write-only — nothing ever deleted entries, so both
    maps grew linearly with the total messages processed and a long-running
    pipeline (10k msg/s for days) drifted toward OOM. The frontier sweep now
    deletes the seqs it passes; the maps stay bounded by the in-flight
    window instead of the emission history.
  - The kafka, file and sql sources rescanned `srcSeq` 1..N on every `Commit`
    call, and the engine calls `Commit` on each frontier advance (~per
    message) — O(N²) total, so throughput decayed over runtime. Each source
    now keeps a per-run watermark of already-scanned seqs and scans only the
    new tail (`toCommit` order, pending deletion and state semantics are
    unchanged; the sql source resets the watermark per pull session, which
    re-numbers seqs from 1).
- **SIGTERM now triggers graceful shutdown** in every long-running verb
  (`run` single-pipeline and `--config-dir`, `mcp`, `replay`,
  `trigger`). Previously only SIGINT was registered, so `docker stop`
  and Kubernetes pod termination killed the process mid-flight without
  the final settle report (at-least-once still held via checkpoint
  recovery, but the clean drain was skipped). `lsp` already handled
  both signals; the others now match it. Verified against the published
  image shape: `docker stop` now logs the settle status line and exits
  0.
- **A third hot-path defect from the same performance review**: the
  commit tracker's advance sweep scanned the ENTIRE in-flight srcRefs map
  (up to the 10k high watermark) on every frontier advance (~per
  message) while a hole pinned the prefix. Source refs are now a FIFO
  ordered by spool seq — arrival is near-ordered (each source goroutine
  registers back-to-back), and the rare cross-source inversion splices
  into place — so the sweep pops the committed prefix from the head
  instead of rescanning everything above it (~28,500ns → ~37ns per
  advance at a full 10k window; `BenchmarkCommitTrackerSweep` locks it
  in). The `committed`/`frontiers` callback maps are unchanged: they
  escape the tracker lock into persistence and reuse would be visible to
  concurrent observers. `snapshot()` is O(1) as well now (the outstanding
  total is maintained incrementally instead of summing the in-flight map
  under the lock on every poll).

### Security

- **The admin HTTP surface (Admin REST + SSE + UI + `/metrics` + `/mcp`)
  now authenticates and validates Host headers** (security review P0: the
  listener previously had no auth and no DNS-rebinding defense, while its
  write endpoints — `POST /admin/deploy` above all — accept pipeline YAML
  whose grpc plugin `command:` executes on the host). New
  `internal/admin.Security` middleware:
  - Optional bearer token, resolved `--admin-token` flag >
    `EVENTBOAT_ADMIN_TOKEN` env > `admin.token` in the Runtime config.
    When set, every request on the listener requires
    `Authorization: Bearer <token>` (constant-time compare) or the UI's
    `?token=` form — EventSource cannot set headers — and gets 401
    otherwise. The read-only console gains a sign-in prompt (token kept
    in sessionStorage); agents and curl should use the header.
  - **Non-loopback binds now REQUIRE a token**: `admin.listen` addresses
    other than 127.0.0.1/localhost/::1 refuse to start without one
    (loopback without a token is unchanged — backward compatible for
    local use).
  - Host header allowlist (DNS-rebinding defense): loopback binds answer
    only the loopback spellings of the configured port; a token-secured
    wildcard bind (`0.0.0.0`/`:port`) skips Host checking since the
    header carries no signal there — the token is the gate.
  - Conservative server timeouts beyond the existing
    `ReadHeaderTimeout`: ReadTimeout 60s, IdleTimeout 120s, and a
    deliberately loose 15m WriteTimeout (SSE responses are long-lived
    streams; the UI's EventSource reconnects).
- **`metadata.name` is validated at the config loader** (security review
  P1: the deployed YAML was written to
  `<data-dir>/pipelines/<name>.yaml` with the name taken verbatim from
  user-supplied YAML, so `../../evil` escaped the pipelines directory).
  Names must now match `^[a-zA-Z0-9][a-zA-Z0-9._-]*$`, be at most 64
  characters and contain no `..`; violations are the new `cfg_name_invalid`
  error (empty names keep `cfg_metadata_name`). The loader is the single
  gate, so CLI, LSP, MCP tools and the Admin REST surface are all covered,
  and verify-first means no file is written for a rejected name.
  **Behavior change**: previously-loadable configs with unusual names are
  now rejected; renames are confined to the `metadata.name` field.
- **Defense-in-depth round two** (the same security review's P2/P3
  findings):
  - Admin request bodies are capped at 8 MiB via `http.MaxBytesReader` in
    the one `body()` helper every JSON endpoint funnels through; oversized
    requests get 413 (two orders of magnitude over any realistic deploy
    config — inline Starlark scripts are tens of KB — while bounding the
    memory one request can pin; server timeouts alone bounded time, not
    bytes).
  - The dead-letter query (Admin REST `GET /admin/dlq/{pipeline}` and the
    MCP `deadletter_query` tool) now applies the SAME `telemetry.redact`
    patterns as the tail — payload patterns against the raw document,
    `meta.*` patterns against the meta map — at the ops layer where both
    surfaces meet. Presentation-only, like the tail: the stored rows stay
    raw so `DeadLetterReplay` re-injects the original bytes.
  - CEL predicates evaluate under a runtime cost limit of 1e6 cost units
    (cel-go `CostLimit`), the CEL counterpart of Starlark's 100k step
    budget: realistic predicates cost O(100) and even a 10k-iteration
    comprehension stays near 1e5, while payload-driven blowups (regex or
    equality over a >10 MB string, >100k-wide comprehension fan-out) are
    cancelled on the EXISTING error path (eval error == condition does not
    pass + counter, no new behavior). CESQL was checked and needs no
    mirror: its host uses the CloudEvents SDK's own parser/evaluator and
    never builds CEL programs.
  - Contract-test suite paths (`pipeline:` and `inject.messages:` fixture
    files) are containment-checked against the suite ROOT — the parent of
    the suite's directory, i.e. the documented
    `<root>/pipeline.yaml` + `<root>/tests/<suite>.yaml` project layout, so
    the conventional `pipeline: ../pipeline.yaml` keeps working
    (`filepath.Rel`-based, volume-aware on Windows): references escaping
    above the root are rejected with an error naming the offending path.
    On the MCP test surface the suite now runs from a fresh
    `<tmp>/tests/` directory, which bounds agent-supplied paths to that
    one temp dir — an MCP client can no longer point the daemon at
    arbitrary files whose contents failure summaries echo back.
  - The LSP server caps one framed message at 16 MiB (four orders of
    magnitude over real document-sync payloads) and treats an oversized
    `Content-Length` as a transport error (connection closed) instead of
    pre-allocating the claimed size.
  - The gRPC plugin transport sets explicit message caps on both sides:
    `rpcplugin.MaxMessageSize` = 64 MiB, applied as dial call options on
    the host and `MaxRecvMsgSize`/`MaxSendMsgSize` on the reference
    plugin server, and documented in docs/plugins.md as part of the
    transport contract. The engine bounds messages by COUNT
    (`limits.max_in_flight`), never by bytes, so grpc-go's 4 MiB default
    would have failed legal large Events with opaque ResourceExhausted
    errors; 64 MiB sits above anything the engine produces today while
    bounding a buggy or hostile plugin's per-message memory.

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
