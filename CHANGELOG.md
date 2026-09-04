# Changelog

All notable changes to Eventboat. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver
(pre-1.0: the API surface may still shift between minor versions).

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
