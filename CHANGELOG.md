# Changelog

All notable changes to Eventboat. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver
(pre-1.0: the API surface may still shift between minor versions).

## Unreleased

The architecture review pass (review-2026-09): one proven P0 concurrency
defect, two engine correctness fixes, an admin security hardening, and the
hygiene findings.

### Added

- **`run.mode: batch` — pipelines that run to completion** (spec v1.24):
  a continuous-shaped pipeline whose sources terminate (a `file` source with
  the new `on_eof: stop`) makes `eventboat run` exit BY ITSELF once the
  engine is quiesced — every source exhausted, nothing uncommitted, every
  commit advance flushed. Exit codes align with `trigger`'s job statuses:
  success=0, completed-with-dead-letters or failed source=1, interrupted=1.
  The completion policy lives in one shared engine primitive
  (`Engine.WaitQuiesced`, quiesce-poll + fatal fold-in) consumed by both the
  batch runner and the jobs runner (the jobs wait loop is refactored onto it,
  which also fixes a latent drop: a worker-fatal racing the quiesce poll is
  now folded into the run's terminal state instead of being discarded).
  Verify warns `batch_no_finite_source` (strict-escalated) when nothing
  declares finite exhaustion — the run would hang. In the config-dir daemon
  a batch pipeline transitions to a new `completed` status (distinct from
  the admin-initiated `drained`), and a failed batch reports `failed` with
  the failure on the pipeline's error field (candidate 02 supersedes the
  original `OnSourceError` callback this entry described); the admin UI
  renders the new state. `run.mode: batch` pipelines take no `parameters:`
  (job-only) and no `schedule`.
- **Finite file sources (`on_eof: stop`) and file-source job eligibility**:
  the file source gains `on_eof: tail|stop` (default `tail` = today's
  tailing semantics, zero change). `stop` is for COMPLETE batch files: once
  the file is read to its end the source returns exhausted — which makes it
  job-eligible (`capabilities: [pull, finite]`; the engine drives `Run` for
  job sources without a `Pull` method and the nil return IS the exhaustion
  signal), so `eventboat trigger` performs one complete read-and-commit pass
  in-process and exits. Under `stop` a missing file is a loud source error
  (mechanically ending the silent-zero-output failure mode); under `tail` a
  missing file now waits inside the poll loop via the existing reopen path
  (previously the source goroutine exited permanently at startup and the
  reopen code was unreachable — the comment promised a behavior the code
  didn't have). The byte offset stays the persisted watermark: re-triggering
  a file job re-reads nothing until the file grows. Verify warns
  `job_file_source_no_eof` (strict-escalated) for job pipelines whose file
  source doesn't stop.
- **Built-in `debug` sink**: prints each message as one line on stderr —
  the "just show me the data" edge for pipeline debugging. An optional
  `prefix` labels the output so fan-out branches can be told apart; stderr
  rather than stdout because the CLI dispatch contract keeps stdout for
  data, and a sink must never interleave with it. A debugging convenience,
  not a production edge: no roll-over, no buffering, and a closed stderr
  fails the batch like any sink error (dead letters / retries apply).
  Registered through the same typed path as the other builtins (schema
  golden pinned; unknown-field rejection covered) with a pipe-capture
  behavior test.
- **`dlq` configuration section with opt-in dead-letter retention**
  (`dlq.retention`, redesign-v3.md §5.10): the `dead_letter` table previously
  grew without bound — its only deletion path was a successful `replay`
  reinjection. A pipeline can now set `dlq.retention` (a duration, `30d`
  style); the engine sweeps dead letters older than the cutoff on the
  checkpoint retention window (piggybacking on the spool trim's window),
  in bounded 10,000-row batches (`Store.DeleteDeadLettersBefore`, SQLite and
  in-memory stores), logging failures and retrying on the next window — the
  commit path never blocks on the sweep, and deleting terminal artifacts
  cannot affect the invariants. **The default is 0 = keep forever, by
  ruling**: dead letters are operator data for `replay`, so automatic
  deletion would silently destroy it; retention must be switched on
  explicitly, and swept rows are gone from `replay` for good. New loader
  diagnostics `cfg_dlq_type` / `cfg_dlq_retention`; the `dlq` top-level key
  is no longer rejected as unknown (the "defined §5.10, not implemented"
  hint is retired); the LSP completes the section. A
  `dead_letter(pipeline, created_at)` index backs the sweep's range scan
  (and `DeadLettersSince`) so a large backlog never degrades to a
  per-pipeline table scan per batch.
- **`run_retention_unset` verify warning**: a job pipeline without
  `run.retention.history` keeps its job_run history forever (one record per
  run, unbounded) — verify now warns, with `--strict` escalating to an
  error, exactly mirroring the `wasm_no_kill_switch` warning/strict
  contract.
- **`/live` and `/ready` health endpoints**: the k8s manifest and docs had
  referenced them since the operator trim, but the mux served neither — the
  shipped example put the pod in a probe-failure restart loop. `/live` is
  the process (200 `ok`), `/ready` flips to 503 once shutdown begins so a
  terminating pod drains first; both are exempt from the bearer token (a
  kubelet probe cannot carry a Secret) while the Host allowlist still
  applies. The example manifest gains the Runtime config that makes the
  listener reachable in-cluster (wildcard bind + token from a Secret).
- **SIGHUP reload** (`run --config-dir`): re-scans the mounted directory and
  deploys the new or changed pipeline files (a file whose bytes match the
  deployed copy is skipped, so a reload is not a drain-and-swap of
  everything); removals need a restart. Unix-only — Windows cannot deliver
  SIGHUP. The k8s rollout docs now describe the semantics instead of
  merely naming the signal.

### Fixed

- **Branch isolation (new invariant 8, `TestInvariant_BranchIsolation`)**:
  fan-out siblings share the underlying `msg.Decoded` / `msg.Meta` maps, and
  the Starlark `remove()` host glue's lazy path deleted from that shared Go
  map in place — a sibling branch encoding the same map concurrently hit the
  unrecoverable `fatal error: concurrent map iteration and map write` (PoC:
  a source fanned out to a `remove`-script transform and a json sink; first
  batch crashed the process). `deleteKey` now materializes the copy-on-write
  tree before deleting, like every other mutation path; the message-ownership
  contract (transforms must replace, never mutate in place) is documented on
  `registry.Message` and `pkg/plugin`.
- **Crash-replay of injected messages** (`replay_inject_test.go`): a spooled
  internal injection (testkit capture, DLQ reinjection) replayed after a
  crash was dispatched as a fan-out from its node — skipping the transform,
  or silently counting as *filtered* when injected at a sink. Replay now
  re-enters the node through the same `dispatchInternal` path as `InjectAt`,
  honoring the previously write-only `meta.injected_at`.
- **Mixed sink-batch timeout**: `writeBatch` took the strictest retry count
  across edges but let the LAST edge's `timeout_ms` win — now the maximum
  explicit timeout wins (the default applies only when no edge sets one).
- **`engine.New` normalizes `WasmSlowCallWarnMs`** like every other option
  (0 → 5000ms; negative = explicitly disabled) — a hand-built `Options{}` no
  longer silently loses the wasm slow-call watchdog.
- **Windows checkouts**: the repository now has `.gitattributes`
  (`* text=auto eol=lf`, binary marks for .descr/.wasm/.db); golden-file
  tests (`TestSchemaGoldens`, `TestHelpSnapshots`) previously failed on any
  autocrlf checkout.
- **Documentation drift from the review-2026-09 pass**: the spec body
  (`redesign-v3.md`) still taught the `from` wiring after the `depends_on`
  rename — the normative sections and examples are now synced (a v1.20
  revision note records the rename and the kept-by-design `from` uses: job
  parameter bindings, `replay --from`); invariant 8 (branch isolation) is
  added to §6.2 and the invariant counts are corrected from seven to eight
  across both READMEs and the developer guides; the transform plugin guide's
  "mutate `msg` in place" guidance is replaced by the message-ownership
  contract it contradicted; the site landing page's status line is updated
  from v0.1.0-beta to v0.3.0.
- **The fanin example ships its input data**: `pipeline.yaml` tails
  `input/orders.jsonl` + `input/refunds.jsonl`, but neither file was ever
  committed — and `file` paths resolve against the process working
  directory, so running from anywhere but the example directory found
  nothing and the source silently produced zero messages (no error: a
  missing file is "nothing to tail yet"). Sample data now ships and a
  README states the run-from-this-directory requirement; end-to-end
  verified (5 messages fan through `stamp` onto `output/audit.jsonl`).
  linear, branching and codecs have the same gap and are still
  contract-test-only.
- **`--ephemeral` surfaces now share one store, and the in-memory store
  actually substitutes for SQLite (candidate 04)**: `--ephemeral` returned a
  fresh memory store per provider call, so the daemon's engine wrote to one
  store while `Status`/`Jobs`/`dlq_query` read another and saw nothing (the
  memory owner now caches one store per pipeline). The in-memory store's
  spool ignored the pipeline argument its comment claimed to honor (SQLite
  keys on `pipeline`), so multi-pipeline stores leaked rows across
  pipelines; the spool is now pipeline-scoped with store-global sequences,
  `JobRuns` sorts by `started_at`/`run_id` like SQLite's `ORDER BY`,
  `RunnableJobRuns` matches its `started_at` order, and a duplicate run id is
  rejected like the `run_id` primary key. A new conformance suite runs the
  same operation sequence against SQLite (temp file) and memory and requires
  identical results, including multi-pipeline isolation. Persisted timestamps
  move to a fixed-width RFC3339 layout: `RFC3339Nano` omits a zero fraction,
  so `"…T12:00:00Z"` sorts AFTER `"…T12:00:00.5Z"` lexicographically while it
  is earlier in time — the `DeadLettersSince`/retention cutoffs and the
  run-history ordering silently misordered rows that differ only in
  sub-second precision.
- **Reinjections that fail again are dead-lettered, never dropped
  (adversarial review 2026-09-24)**: `dispatchInternal` delivered injected
  messages on a zero-value synthetic edge (`Required: false`), so a
  reinjection that failed again at its sink took the optional-drop path —
  and since `replay --delete` removes the original record once the
  reinjection commits, the message was silently lost. The synthetic edge is
  now required: a re-failure writes a fresh durable record.
- **The lazy codec cache is mutex-guarded (adversarial review 2026-09-24)**:
  `Engine.codec` wrote its resolution cache without a lock while source
  goroutines, sink workers and operator replays resolved concurrently; a
  foreign codec name (a replayed row whose codec the running config no
  longer declares) could be un-cached, and two concurrent resolutions were a
  concurrent-map-write panic. Also: an explicit empty `edge_defaults:` (YAML
  null) is accepted as an empty declaration instead of a type error.

### Changed

- **The SSE `status` event has one payload shape (rethink follow-up
  2026-09-24)**: the transition events (pause/drain/resume/engine
  completion) carried a bare pipeline name while the periodic ticker carried
  the full snapshot, so the read-only UI rendered the string as a table.
  Both now carry the `[]PipelineStatus` snapshot.
- **One owner for the durable store of a pipeline (candidate 04)**: where a
  pipeline's store lives and how long its handle lives now belong to one
  module, `store.Owner` (`internal/store/owner.go`), and every entry point
  opens through it. The canonical layout is
  `<data-dir>/stores/<sanitized pipeline>.db` for the daemon and the one-shot
  verbs alike — a run history written by `trigger` is the one `jobs list` and
  the daemon's `Status` read — and the old `eventboat.db` /
  `stores/pipeline.db` layouts are retired without migration (beta ruling:
  the old files are simply no longer read). The owner caches one handle per
  pipeline per process and closes them all on `Close` (idempotent); it also
  exposes `Path` — stage 08's cross-process lease locks exactly that file.
  `ops.Options.StoreFor` is deleted together with its default factory (the
  `stores/pipeline.db` layout ignored the pipeline argument); the service now
  takes an injectable `store.Provider` (`ops.Options.Stores`) and `New`
  refuses a missing provider instead of silently falling back to a layout.
  The 23-method `Store` interface is split into facets — `SpoolStore`
  (spool/checkpoint/source states), `DeadLetterStore`, `JobRunStore`, with
  the combined `Store` kept for consumers that span concerns — pinned by
  compile-time assertions for both implementations; `engine.New` now depends
  on the spool + dead-letter facets and `jobs.New` on run history plus the
  engine's facets, so neither can reach the handle lifetime. The
  file-name rules (`Sanitize`, `WindowsReservedName`) move to the shared leaf
  `internal/fsname`, imported by the loader's `metadata.name` validation and
  the store owner alike — one reserved-name list, no copies.

- **One run outcome for every runner (candidate 02: `Engine.Wait` /
  `Outcome`)**: every runner re-derived the terminal state around
  `WaitQuiesced` (jobs read a racy `OnSourceError` callback, batch read
  `SourceErrors`, the daemon polled `Quiesced` behind a 100ms settle window
  and reported a failed batch as still `running`). The engine now owns the
  decision: `Wait(ctx, runDone, WaitOptions)` encapsulates quiesce → cancel →
  bounded wait for `Run` → classification, and `Outcome` carries the status,
  rows read/committed/dead-lettered, the `SourceErrors` snapshot, the
  worker-fatal error and the abandon count/error. `RunStatus` is `completed |
  partial | failed | interrupted` with the priority worker-fatal → source
  errors → caller cancellation (the ctx passed to `Wait`, never the engine's
  internal cancel) → dead letters > 0 → completed, so an engine self-stop is
  never misreported as interrupted. Consumers map the outcome: job runs to
  `success`/`partial`/`failed`/`canceled` (on cancel: abandon first —
  `AbandonError` escalates to `JobFailed`), batch to exit 0/1/1/1, the
  continuous `run --config` to exit 1 only on a failed run and 0 on
  SIGTERM/SIGINT (the long-lived contract), the daemon to
  `completed`/`failed` (a failed batch reports `failed` with the failure
  text; the settle poll and the source-error SSE callback are gone).
  `Quiesced` gains the `replayDone`/`admitting` guards, so an injected or
  admission-blocked message can no longer read as quiesced. **A genuine
  source failure now stops the engine in every mode** — a continuous pipeline
  must not keep running with a dead source; restart resumes from the source
  watermarks (duplicate delivery, never loss), and an error returned under an
  already-cancelled engine ctx is a shutdown artifact, not a failure.
  `Options.OnSourceError` is deleted (jobs, ops and the docs updated):
  `SourceErrors`, carried by the outcome, is the only channel.
  **`Abandon` is now `Abandon(ctx, reason)`**: bounded by the caller's ctx
  (`WaitOptions.AbandonTimeout`, default the engine's `DrainTimeout`), the
  durable record is written FIRST and the tracker is force-terminated only
  after a successful write; a ctx cancellation or store failure stops the
  attempt, leaves every unrecorded message uncommitted and returns an error —
  the next run replays them (never loss). The normal dead-letter path keeps
  retrying forever on the engine ctx (invariant 4); only Abandon is bounded.
  New tests: the four-status classification matrix and end-to-end outcomes,
  the source-failure self-stop in continuous mode (engine + a binary-level
  exit-1 acceptance), the bounded abandon with a failing dead-letter store
  (messages stay uncommitted, the next run replays them) and the
  record-before-force-terminate ordering, the Quiesced guards (replay scan,
  in-flight admission, injection scenario), the exit-code mapping table and
  the ops batch status machine (`completed`/`failed`).

- **BREAKING: `registry.Source.Run` returns `error`** (pkg/plugin aliases
  follow; compiled-in plugins must be rebuilt, v1.18 precedent). The return
  value is the source's completion signal: nil = exhausted or voluntary
  stop, non-nil = failed source — recorded in `SourceErrors` exactly like a
  pull failure (previously the engine hardwired push-source
  completion to "done, no error" and a failed source was indistinguishable
  from a stopped one; the gRPC adapter's comment said so in as many words).
  ctx cancellation is defined as a VOLUNTARY stop — sources return nil, never
  ctx.Err() — and all builtins now honor it (the sql source previously
  surfaced wrapped cancellation errors as failures). The out-of-process gRPC
  protocol is UNCHANGED: a clean Run-stream end already mapped to
  exhaustion on the wire; the host adapter now propagates it instead of
  swallowing it. `http_server` sources now fail loudly on bind errors (port
  occupied) instead of returning silently. A failed source is recorded for
  every source kind, not just pull sources (candidate 02 deleted the
  `Options.OnSourceError` callback this entry originally added — the
  `SourceErrors` snapshot carried by the run outcome is the only channel).
- **BREAKING: the source emit callback returns `error`**
  (`Source.Run(ctx, func(Message) error) error`, `PullSource.Pull` likewise;
  `pkg/plugin` aliases follow; compiled-in plugins must be rebuilt). The
  error is the **admission verdict** (candidate 01): nil = the message was
  durably accepted; a ctx error = engine shutdown (the source returns nil —
  a voluntary stop); anything else = a **refusal** — not durable, never
  visible, safe to re-emit. The source owns its input semantics and decides
  whether to retry internally or fail; the engine retries nothing. Builtin
  default: report the refusal as a failed source (the source watermark is the
  no-loss safety net); `http_server` answers 503 instead. `file_source`
  deliberately keeps its lock-across-emit shape as the regression fixture.
  The out-of-process gRPC protocol is UNCHANGED; the host adapter surfaces a
  refusal as the stream's error (the last Commit state is untouched, so the
  plugin re-emits it).
- **BREAKING: `Engine.InjectAt` / `Engine.InjectReplay` take a
  `registry.Message`** (Raw/Meta/Codec/ID) instead of `(raw, meta[, id])`.
  Identity and codec travel with the message: `InjectReplay` stamps
  `is_replay`/`original_message_id` from `msg.ID` and preserves it, and a csv
  dead letter now replays as csv (the old internal-injection path hardcoded
  json). Callers updated: `ops.DeadLetterReplay`, `eventboat replay` (its
  items carry the dead letter's / spooled row's codec), `internal/testrun`
  (injects at a source with the node's decoder), the soak driver.
- **`Run` starts its consumers before crash replay** (`startWorkers` →
  `replaySpool` → `startSources`): a recovery with more uncommitted spool
  rows than a node channel holds used to hang forever on a channel nobody
  read. The replay callback treats ctx cancellation as a voluntary stop and
  never surfaces it as an error.
- **Admission is one module with three modes** (`internal/engine/admission.go`,
  live / replay / inject) owning the backpressure gate, the acquired-slot
  ledger, stamping, spool append, commit registration and dispatch; `accept`,
  `injectAt` and the replay callback are thin shells. Replay rows now take
  admission quota, and `Abandon` releases the slots its force-terminate path
  used to leak.
- **Source watermarks persist asynchronously** (`internal/engine/sourcecommit.go`):
  one committer per source receives coalesced maximum frontier advances and
  calls `Source.Commit` off the committing goroutine, so a source may hold an
  internal lock across `emit` without deadlocking the pipeline (the
  backpressured file source used to deadlock against its own commit callback).
  A failed commit or state write keeps its pending value and retries on the
  next advance; `Run` flushes pending frontiers after drain with a bounded,
  non-cancelled context. `Source.Commit`'s interface and semantics are
  unchanged; a source state that lags the checkpoint only widens the crash
  replay window — duplicate delivery, never loss.
- **Breaking: the config `apiVersion` is renamed from `eventboat/v3` to
  `eventboat/v1`** — the version stamps the config format's own generation,
  not the redesign document that produced it; `v3` leaked the internal spec
  numbering (redesign-v3) into the public config surface. Hard switch, no
  alias (POC, no backward-compat obligation): configs still using the old
  value fail verify with `cfg_api_version` (and the runtime-config loader
  errors) pointing at the new spelling. The loader, runtimecfg, all six
  examples, both READMEs, the developer guide, and the spec (v1.23 revision
  note) are synced.
- **Breaking: the node wiring field `from` is renamed to `depends_on`** —
  transforms and sinks declare their upstream nodes with
  `depends_on: [upstream]` or `depends_on: { upstream: { when: '...' } }`;
  sources take no in-edges and never declare it. Hard switch, no alias:
  configs still using `from:` fail verify with the new `cfg_from_renamed`
  diagnostic pointing at the new spelling. Error codes rename with the key:
  `cfg_missing_from` → `cfg_missing_depends_on`, `cfg_bad_from` →
  `cfg_bad_depends_on`, `cfg_source_with_from` → `cfg_source_with_depends_on`.
  The LSP completes/hovers `depends_on` and offers edge-attribute completion
  inside its mappings; `from` stays a reserved plugin name so old configs
  get the migration message instead of a plugin-name clash. Unaffected:
  job-run parameter bindings named `from` (`parameters:` /
  `trigger --parameters`) and the `eventboat replay --from` spool flag.
- **Atomic run admission, started-instance semantics and the pipeline store
  lease (candidate 08)**: the jobs manager's overlap admission was a TOCTOU
  check — the lock was released before the run record was created and the run
  registered, so two concurrent triggers could each observe an empty active
  set and run together under `overlap: skip`/`latest`. `Manager.spawn` is now
  ONE critical section (apply overlap → create the run record → register the
  run goroutine), `Manager.Stop` waits until every run **persisted** its
  terminal state, and `ops.managed.done` genuinely means the instance
  stopped — `Drain`/`Pause`/`Deploy` return drained, and a replacement
  manager's crash recovery cannot resume a run the old instance was still
  driving. Deploy also waits for `jobs.Start` (crash recovery + catchup)
  before the replacement instance becomes visible, closing a deploy/trigger
  race that could start two engines for one run id. A running engine now
  holds an exclusive OS file lock — the **store lease** — on the sidecar
  `<store>.lock` next to the canonical database (`store.Owner.Acquire`;
  `flock` on Unix, `LockFileEx` on Windows, build-tagged, no new dependency).
  The lock is released by the kernel when the process dies (no TTL
  heartbeat), it never touches SQLite's own locking, and the memory owner
  returns a no-op lease so single-process tests and `--ephemeral` are
  unchanged. The daemon acquires the lease per deployed pipeline; the
  write-capable one-shot verbs (`run --config`, `trigger`, a live `replay`)
  acquire it for their run and **refuse loudly when they cannot** — the
  message names the conflict and points at the admin/MCP surface instead of
  starting a second engine on the same spool. A Deploy while the same process
  already runs the pipeline transfers cleanly under a per-pipeline lifecycle
  mutex (old releases, new acquires); a different process holding the lease
  makes Deploy fail loudly. The daemon's per-pipeline instance now moves
  through an explicit state machine — `running`, `paused`, `drained`,
  `completed`, `failed` — serialized by that same lifecycle mutex:
  `Resume` from `drained` actually RESTARTS the pipeline (the old silent
  no-op reported `running` while nothing ran), terminal instances refuse
  Pause/Drain/Resume and are replaced by Deploy, and one status derivation
  reports the LATEST runnable run instead of the oldest. Instance contexts no
  longer derive from the Deploy/Resume CALL context (an MCP/HTTP request
  finishing must not stop a deployed pipeline). An async `trigger`
  (`wait=false`) returns the created run record — `run_id`, never `null` —
  across jobs/ops/admin/MCP. `maybeCatchup` locates the newest in-window tick
  by bisecting the schedule (~log2(outage) probes) instead of materializing
  an unbounded missed-tick list; `eventboat_jobs_catchup_skipped_total` now
  counts one skipped episode (counted once — exact per-tick counting is the
  unbounded walk the bisection exists to avoid). Docs: 02-engine (recovery +
  single writer), 06-observability (catch-up counter).
- **One verify-first path and unified DLQ semantics (candidate 05)**: the
  load → build → judge composition lived at about ten call sites with
  different policies — `ops.Verify` always built the IR (cascade diagnostics
  the CLI skipped), `--strict` existed only in the CLI, the LSP dropped the
  document URI so relative paths resolved against the editor process's CWD,
  and eight hand-rolled severity scans decided pass/fail. It now lives in one
  cycle-free package, `internal/verify`: `File`/`Bytes` return a `Result`
  carrying the typed config, the built `*ir.Pipeline`, the merged
  load+build diagnostics and the strict verdict `OK`, while
  `LoadBytes` → (parameter substitution) → `Build` is the two-stage form jobs
  uses; a load that already errored skips the build, uniformly. Content-based
  entries take an explicit `baseDir` (`config.LoadBytesIn`): the LSP passes
  the document's directory, Deploy the deploy directory (relative
  wasm/grpc/codec paths now resolve the same at deploy time and on every
  per-run reload), the CLI the file's directory, and a pure-text MCP/Admin
  submission `""` (documented: relative paths then resolve against the
  process CWD) — the engine and IR read the same `Pipeline.BaseDir` instead
  of re-deriving paths from the file name. `config.Diagnostics` is a
  first-class value with `HasErrors`/`FirstError`/`StrictOK`/`Errors`/
  `Warnings`, and every severity scan is gone. `internal/dlq` owns the
  dead-letter strategy surface: where-filter compilation with the pipeline's
  constants (the CLI compiled with constants while MCP did not), selection
  order `ids → where → limit`, and the codec-carrying replay request shared
  by the live-engine (ops) and local-engine (CLI) transports — **`--ids`
  with `--limit` no longer deletes dead letters that were never replayed**
  (only the selected rows are eligible for `--delete`). Explain has one
  Service-free entry (`ops.ExplainPipeline`, `ExplainRequest.Message/
  EntryNode/Topology`), so `--at` behaves identically on CLI, MCP (`explain`
  gains `at`) and Admin; the CLI delegates instead of re-implementing.
  `ops.Deploy` parses once: verify in memory → write the deployed file →
  swap the instance through the candidate-08 lifecycle, passing the built IR
  to the new engine — no third parse, and a rejected deploy writes nothing.
  Docs: 01-architecture, 04-config-pipeline, 06-observability.

- **One framework vocabulary (candidate 06)**: the framework-field whitelist
  was hand-copied into five modules and had drifted — `grpc` and `version`
  were node fields in config yet registrable as plugin names, so such a
  plugin could register and never load. The single source is now the leaf
  package `internal/framework` (per-section node fields, top-level keys, edge
  attributes, reserved plugin names, node-level default constants); config,
  registry and the LSP read it, and a test pins the reserved set to the
  union of the framework fields. `Pipeline.Order` is now the **YAML document
  order** of node declarations (previously Go map iteration): jobs binds
  `cursor` to the first declared source's watermark, explain's default entry
  node is the first declared source, and diagnostics iterate a stable order.
  Pipeline-level defaults are materialized into the typed config at load
  (decoder/encoder `json`, delivery `retries: 3`/`backoff: exponential`,
  `required: true`, buffer `max_events: 128`, workers 1) and the downstream
  re-defaulting (the `json` fallback ten times over, the IR edge defaults)
  is deleted; engine runtime knobs keep exactly one normalization path
  (`Options.withDefaults`, shared by `DefaultOptions` and `New`).
  `edge_defaults` rejects `when`/`route` with `cfg_edge_defaults_field` (a
  global default predicate is a footgun; `delivery`/`required`/`buffer`
  unchanged). Dead knobs are closed: **BREAKING** `sinks.workers` — accepted
  and ignored before — is rejected with `cfg_sink_workers` (sink concurrency
  is engine-owned), and `mcp.enable: false` means the daemon does not
  register `/mcp` (the explicit `eventboat mcp --http` command always
  serves it, documented). Strictness parity: `metadata` gains unknown-key
  (`cfg_unknown_field`) and non-mapping (`cfg_metadata_type`) diagnostics,
  and `runtimecfg` moves to a typed strict decode where unknown keys AND
  type errors are errors (`data_dir: 123`, `enable: "yes"` and
  `sample_ratio: -1` used to fall through to defaults; `sample_ratio` is
  now enforced in `[0, 1]`). Docs: 01-architecture, 03-plugins,
  04-config-pipeline, 06-observability.

- **Typed failure kinds across the transform seam (candidate 07)**: the
  engine classified transform failures by matching message text produced in
  other packages — `strings.Contains(serr.Msg, "too many steps")` and
  `strings.Contains(err.Error(), "exceeded")` — so a guest error containing
  "exceeded" counted as a wasm timeout, `TransformError.Flavor` was a second,
  unused flavor channel (split filled it without implementing the
  interface), and `obs.ReasonClass`'s prefix list was already wrong: an
  `"encode: "` reason never matched its `"encoder"` prefix and landed in
  `other`. Hosts now type their failures **where the failure is created**:
  `starhost.ScriptError.Kind` (compile/steps/runtime — budget exhaustion is
  set by the Starlark step-limit hook, never sniffed from the message text)
  and `wasmhost.Error.Kind` (compile/timeout/guest/trap; the per-invoke
  budget error and every other host error is typed at creation). The builtin
  adapters map the host kind onto the new `registry.FailureKind` enum
  (steps/timeout/guest/compile/runtime/other) carried by
  `TransformError.Kind`; the `"wasm: "` prefix stripping goes away and the
  host's message text is preserved verbatim. **BREAKING**: `TransformError`
  loses `Flavor` and `Flag` (compiled-in plugins rebuild, v1.18 precedent) —
  flavor travels only through the `TransformFlavor` interface, which split
  now implements, and the engine's flavor switch gained the documented
  generic default: known flavors (`script`, `wasm`) feed their per-flavor
  duration histograms and kind-driven budget/timeout counters, while an
  unknown flavor is recorded generically (run accounting plus the
  dead-letter class) instead of being silently ignored. `store.DeadLetter`
  gains **`Class`**, recorded where the dead letter is produced — `decode`,
  `codec`, `encode` (the old `encoder`/`encode` reason-prefix split is
  resolved into explicit classes), `delivery`, `canceled` for abandoned
  messages, and the transform failure kinds — and both store
  implementations persist it (the SQLite column is added in place for
  existing databases; the `class` field joins the dead-letter JSON).
  `obs.ReasonClass` is deleted: `RecordDeadLetter` receives the class the
  engine already knows, so **the `reason_class` metric label values
  change** — `"script"` becomes the failure kind, `"encoder"` becomes
  `codec`, an `"encode:"` failure is now `encode` (never `other`), and
  cancel/abandon records are `canceled`. New tests: the host kind matrices
  (script compile/steps/runtime; wasm compile/timeout/guest/trap against a
  hand-assembled guest module), the adapter mapping, a grep gate against
  text classification in starhost/wasmhost/builtin/engine/obs, the
  false-positive regression (a guest error containing "exceeded" stays
  guest), the dead-letter class on every production path with metric-label
  assertions, and the unknown-flavor generic-branch pin. Docs: 02-engine,
  03-plugins, 05-scripting, 06-observability, wasm.md.

- **Explain renders resolved semantics (candidate 09)**: the walkthrough
  claimed to match production but re-derived per-tier knowledge by plugin
  name. `explain` now reads the resolutions production uses: a wasm node
  without `timeout_ms` shows **fast mode (no per-invoke kill switch)** instead
  of a fictional 1000 ms budget (`wasmhost.ResolveMode` is the one resolution
  the compiler, the invoker and the renderer share), a sink renders **every**
  inbound edge's delivery policy plus the engine's mixed-batch rule (max
  retries and max explicit timeout; the default timeout applies only when no
  edge in the batch sets one), a `required: false` edge shows its terminal
  **drop** (the engine drops and commits) instead of always "dead letter",
  the sample message is decoded with the entry source's **declared decoder**
  (the same codec instance the engine resolves — a csv/raw source no longer
  feeds JSON, and the `decoder %s` header claim is true), and each edge
  predicate is evaluated exactly once per walk (it used to be evaluated twice,
  so a stateful predicate could disagree with itself). Instance lifecycle is
  split: the default verify-only build (`verify`, LSP, jobs, testrun) closes
  every transform instance immediately — **including on failure paths**, so
  the leak class is gone — while `verify.Options.ForExplain` retains the
  explain-safe instances for dry-runs and the explain callers (CLI, MCP,
  Admin, `replay --dry-run`) close the pipeline through the new
  `ir.Pipeline.Close`. `wasmhost` caches compiled modules keyed by file
  identity (path/size/mtime) plus the compile-affecting config bits (memory
  cap, kill switch): a changed file or config recompiles, an unchanged one is
  reused, so the LSP no longer recompiles a guest on every keystroke; the
  cache is bounded (LRU) and reference-counts `Compiled`, so evicting an
  entry a live invoker still uses is safe. New tests: the divergence set, a
  comparison matrix that runs the same IR through the real engine
  (testkit-driven) and requires the walkthrough's predictions to equal the
  terminal states (when/route, zero-match filtering, optional drop, transform
  dead letters, split expansion), the Init/Close lifecycle counters for both
  build modes and the failure path, and the cache hit/invalidation/config
  separation. Docs: wasm.md, 04-config-pipeline, 03-plugins.
- **Admin security hardening**: the `?token=` query form is accepted on
  `/admin/sse` only (EventSource cannot set headers); every other endpoint is
  header-only, so a token leaked in a URL no longer unlocks the write
  surface. All responses carry `X-Content-Type-Options: nosniff`.
- **`Engine.Run` is single-shot** (a dedicated guard flag): a second call
  returns an error instead of re-replaying the spool and duplicating workers;
  the readiness flag `started` remains a post-ctx-assignment publication so
  `Ready()`/`injectAt` never observe a not-yet-assigned context.
- **`Engine.Abandon` returns `(int, error)`**: a store failure mid-abandon is
  propagated (the job runner fails the run) instead of silently reporting a
  clean cancel.
- Transform channels size their buffer with the same surge-cap rule as sink
  channels (`BufferMax` participates only below 4× the base capacity).
- **Dead-letter records drop the never-populated `origin_node` and
  `retry_count` fields** (and their JSON shape); the SQLite columns remain
  (defaulted) for old databases.
- `store.NewMemory()` takes no pipeline argument — the in-memory store was
  effectively pipeline-agnostic and the parameter suggested otherwise.

### Removed

- **`registry.TransformError.Flavor` and `.Flag`** (candidate 07): flavor is
  an instance property (read through `TransformFlavor`), and the failure kind
  is typed on the error (`Kind`) where the failure is created.
- **`obs.ReasonClass`** (candidate 07): the dead-letter class is recorded at
  production time on `store.DeadLetter.Class`; metrics read it directly, so
  no prefix list can drift from the reasons again.
- **`store.JobCommitting` and `JobRun.Runnable()`** (candidate 08): the
  reserved `committing` state had no writer, so it was deleted from the enum,
  the runnable predicate, the SQLite status sets (`RunnableJobRuns`,
  `DeleteJobRunsBefore`) and the docs. The runnable set is `pending`/`running`
  (now spelled `store.IsRunnableStatus`; the SQL mirrors it literally, and a
  store test pins both backends against the same six-status enum).
- Dead code: the hand-rolled insertion sort in `internal/ir`, the
  `var _ = strings.TrimSpace` placeholder in `internal/admin`, the unused
  `workers` computation in `engine.New`.

## v0.3.0 (2026-09-05)

The post-rc release line: transforms join the registry as first-class
plugins (spec v1.19), plugins extend at compile time (the root package's
`RunCLI` + `pkg/plugin` custom builds), the out-of-process stubs move to
`pkg/pluginproto`, and the developer guides ship on the documentation site —
with the security and performance audit fixes folded in. Two external-ABI
breaks for plugin authors: the gRPC stubs' import path, and the
Settled → Commit rename (rebuild external plugins against this tag).

### Added

- **Compile-time plugin extension (Benthos-style custom builds)**: the root
  package gains `RunCLI` — a custom `main` blank-imports plugin packages
  and runs the full CLI; `pkg/plugin` is the plugin ABI (aliases of the
  engine's plugin interfaces) plus typed `RegisterSource` /
  `RegisterTransform` / `RegisterSink` / `RegisterCodec` into the
  process-wide registry, so a registered plugin is a first-class citizen of
  the verify gate, `plugin catalog`, LSP and MCP surface. The CLI verb
  table moved from `cmd/eventboat` to `internal/cli`, shared by both entry
  points (`cmd/eventboat` is now a thin main; `cli.Run` returns the exit
  code). Runnable reference:
  [examples/custom-build](examples/custom-build), built and driven from
  outside the module by the root package's `TestCustomBuildAcceptance`.
- **Developer documentation** ([docs/developer/](docs/developer/)): nine
  guides (architecture through contributing) with YAML frontmatter
  (`title:` + `order:`) that drives ordering on the documentation site.
- **Documentation site**: [tools/sitegen](tools/sitegen) — a separate Go
  module (zero-CDN static generator: goldmark, embedded templates, one
  hand-written stylesheet) that renders `docs/developer/`, `docs/*.md` and
  the landing page into a flat, relative-URL tree, deployed to GitHub Pages
  by [.github/workflows/pages.yml](.github/workflows/pages.yml); live at
  <https://eventboat.github.io/eventboat/>.
- **Developer guides on eventboat.dev/docs**: `sitegen -hugo-out` renders
  `docs/developer/*.md` into Hugo content (frontmatter conversion, guide
  cross-links rewritten to section URLs, duplicated title heading dropped);
  the organization site's Pages workflow runs it at build time against this
  repo, so the guides publish there with no copied content to drift.

### Changed

- **`pkg/pluginv1` renamed to `pkg/pluginproto`** (generated from the same
  `proto/eventboat/plugin/v1/plugin.proto`, go_package updated, stubs
  regenerated with the pinned toolchain — protoc-gen-go v1.36.11,
  protoc-gen-go-grpc v1.3.0). The old name read as "plugin API v1" next to
  the registration API; the new name says wire-protocol stubs. Out-of-process
  plugin authors must update the import path and the `pluginv1.` qualifier
  (see the updated reference implementation
  [examples/plugins/ticker-source](examples/plugins/ticker-source)); the
  wire protocol itself is untouched.
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
  filters the message (committed + NoMatch counter, the same semantics as an
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

- **Three hardening findings from the adversarial re-review of the security
  round.** The LSP header read is now bounded: a hostile stdio client could
  grow the heap with megabytes of unterminated header line before the
  16 MiB Content-Length cap ever got to run — header lines are capped at
  4 KiB (LSP headers are a few dozen bytes) and overflow is a transport
  error that closes the connection, the same path as an oversized
  Content-Length. `mcp --stdio` no longer validates the admin surface it
  will never start: the `admin.NewSecurity` check (refuse non-loopback
  binds without a token) ran unconditionally, so a pure stdio MCP session
  was refused whenever `admin.listen` was non-loopback without a token,
  although stdio has no admin listener at all; the check now runs exactly
  when the surface starts (the `--http` path — the same only-when-enabled
  guard `run --config-dir` applies via `admin.enable`), and the refusal
  itself is unchanged. `metadata.name` now rejects Windows reserved device
  names (CON, PRN, AUX, NUL, COM1-9, LPT1-9 — case-insensitive, also as
  the stem before a dot: `con.yaml`), which the deploy persist would
  otherwise write straight to the device instead of
  `<data-dir>/pipelines/<name>.yaml`; the `mcp` store-file sanitizer
  applies the same shared check (`config.WindowsReservedName`).
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
  the final commit report (at-least-once still held via checkpoint
  recovery, but the clean drain was skipped). `lsp` already handled
  both signals; the others now match it. Verified against the published
  image shape: `docker stop` now logs the commit status line and exits
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
