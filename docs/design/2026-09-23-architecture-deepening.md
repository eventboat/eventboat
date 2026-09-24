# Architecture deepening program — 2026-09-23 review

| 状态 Status | 日期 Date | 关联 Links |
|---|---|---|
| Implemented (stages S1–S7) — all eight candidates landed | 2026-09-23 | `CONTEXT.md` (domain vocabulary) · `CHANGELOG.md` (per-candidate entries) |

This document is the design of record for the architecture review held on
2026-09-23. It covers eight deepening candidates (the ninth was absorbed),
the decisions taken during review, the rationale behind each, the staged
implementation plan, acceptance tests and known risks.

**Compatibility ruling.** The project is in beta. Backward compatibility is
not a constraint for this program: hard switches, breaking plugin-ABI
changes and schema changes are allowed, provided every break is recorded in
`CHANGELOG.md` and the affected examples/tests are updated in the same
commit. This ruling decided several contested points below.

---

## 1. Background and scope

The review walked the codebase's hot spots (the last ~80 commits: the engine
completion work, config/IR, jobs, CLI, registry, LSP), then explored four
areas in depth — engine/store, config/IR/registry, jobs/ops/CLI,
hosts/tooling — and verified every load-bearing finding against the code
before proposing anything.

Nine candidates were produced; the user chose to merge candidate 03 (source
seam) into candidate 01, so the program has eight candidates. Each was then
walked through a decision-by-decision review; every decision below was
explicitly confirmed.

| # | Candidate | Strength | Status |
|---|---|---|---|
| 01 | Start the consumers before replaying (absorbs 03) | Strong | Implemented (stage S1) |
| 02 | One outcome for every runner | Strong | Implemented (stage S2) |
| 04 | One owner for the durable store of a pipeline | Strong | Implemented (stage S3) |
| 05 | One verify-first path — the CLI stops re-implementing ops | Strong | Implemented (stage S5) |
| 06 | One framework vocabulary — whitelist, order, defaults | Worth exploring | Implemented (stage S5) |
| 07 | Typed failure kinds across the transform seam | Worth exploring | Implemented (stage S6) |
| 08 | The jobs manager owns run admission and terminal transitions | Worth exploring | Implemented (stage S4) |
| 09 | Explain renders resolved semantics | Speculative | Implemented (stage S7) |

Out of scope by decision (see §6): dropping `Source.Commit` entirely, the
source-lock contract as the fix for re-entrancy, full CLI delegation to
`ops`, implementing `sinks.workers`, cross-process delegation to the admin
API, and migrating old on-disk data files.

## 2. Program order and dependencies

Recommended order, dependency-driven:

1. **01** — foundational: the admission module, emit contract and source
   committer reshape the engine's core. Candidate 02 builds on its source
   failure semantics; 04's lease interacts with engine startup.
2. **02** — terminal semantics (`RunOutcome`): 08 maps run terminal states
   from it; ops status derives from it.
3. **04** — store owner, facets and layout: 08's lease locks the pipeline
   store file, so the owner must exist first.
4. **08** — run admission, lease, instance state machine (amends 04 with the
   single-writer guarantee).
5. **05** — verify path: independent, but it rewrites `ops.Deploy`,
   `ops.Explain` and the CLI verbs that 08 also touches — doing it after 08
   avoids churn.
6. **06** — framework vocabulary: independent; 09 reads the defaults it
   materializes.
7. **07** — typed failure kinds: independent; touches the engine, obs and
   the dead-letter schema.
8. **09** — explain: depends on 06 (materialized defaults), 05 (`ForExplain`
   build mode) and 07 (failure kinds in dry-run output).

Each candidate's implementation plan (§3) is staged so every stage compiles
and keeps the suite green on its own. The repo convention applies: one
logical change per commit, detailed paragraph-led messages, `CHANGELOG.md`
Unreleased entries at commit time.

---

## 3. Candidate designs

### 3.1 Candidate 01 — Start the consumers before replaying (absorbs 03)

**Problem.** `Run` replays the whole uncommitted spool synchronously before
any worker goroutine exists (`internal/engine/engine.go:570-624`); `deliver`
blocks on a node channel (128 by default), so crash recovery with more than
one channel's worth of uncommitted rows hangs forever. No test replays more
than a handful of rows. The replay callback is a third dispatch variant: it
skips stamping, spans and admission, `injectAt` hardcodes the codec to
`json`, and the dead-letter replay path drops `dl.Codec` even though
`store.DeadLetter` carries it. `accept`'s refusal error is discarded at both
`emit` call sites (`engine.go:656,660`), so a refused message is
indistinguishable from an accepted one. And `file_source.pump` holds its
mutex across `emit` while `Source.Commit` takes the same mutex from the
committing goroutine — a backpressured file source deadlocks against its own
commit callback.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | Scope: ordering fix + unified admission + emit contract + codec identity (03 absorbed) | The hang, the silent refusal and the codec loss are one missing module: "how a message enters the DAG". |
| 2 | `emit func(Message) error` hard switch. nil = durably accepted; ctx error = shutdown (source returns nil); other error = refusal (not durable, not visible, safe to re-emit) | The source owns its input semantics (Kafka offset, file offset, HTTP response); only it can decide retry vs failure. |
| 3 | Engine retries nothing; builtin sources report a refusal as a failed source by default | The source watermark is the no-loss safety net; a failed run is louder than a silent zero-output run. |
| 4 | Replay rows take admission quota; fix the `forceTerminal`/`Abandon` quota leak | Uncommitted rows ≤ HighWatermark by construction, so replay cannot wedge; the quota becomes uniform. |
| 5 | Injection API takes `registry.Message`: `InjectAt(node, msg)` / `InjectReplay(node, msg)` | The message already carries Raw/Meta/Codec/ID; identity and codec travel naturally, signatures stay two-parameter. |
| 6 | Independent `admission` type (`internal/engine/admission.go`), modes live/replay/inject | One interface for the whole entry path; differences become request fields, not code paths. |
| 7 | `Run` phases: `startWorkers()` → `replaySpool()` → `startSources()`; regression test pins it | The ordering invariant becomes structural; the test replays more rows than a channel can hold. |
| 8 | Commit re-entrancy: **overturn the lock-contract approach**; per-source async committer with a coalesced maximum frontier; failed commits keep their pending value; shutdown flushes after drain; `Source.Commit` interface unchanged | A framework guarantee beats a rule every source author must obey. A source may hold a lock across `emit`; only watermark persistence lags. |

**Design outline.**

- `admission` owns the backpressure gate, the `acquired` ledger, stamping,
  spool append, commit registration and dispatch. Request fields carry the
  mode differences: whether to append (replay has a seq), whether to
  re-stamp (replay keeps original stamps), codec source (node decoder /
  message / spool), whether to register a source-watermark reference, and
  whether the entry is a fan-out or an into-node delivery.
- `accept`, `injectAt` and the replay callback become thin assembly shells
  over `admit`.
- The replay callback treats ctx cancellation as a voluntary stop (v1.24
  definition) and returns nil, not an error.
- Source committers are per-source workers created in `Run`; `persistCheckpoint`
  writes the checkpoint synchronously (the durable barrier is unchanged) and
  posts frontiers. The `srcPersisted` guard moves into the committer.
- Builtin sources gain refusal handling: file/sql/kafka/cron/rpc return the
  error (source failed); `http_server` answers 503; testkit sources return
  it. `file_source` keeps its lock-across-emit shape deliberately — it is
  the regression fixture proving the framework handles it.

**Implementation steps.**

1. emit contract: `registry.Source.Run` / `PullSource.Pull` signatures; all
   nine builtin/testkit implementations, `rpcplugin` adapter, the
   `examples/plugins/ticker-source` module, `pkg/plugin` docs; CHANGELOG
   BREAKING entry.
2. `admission` type + `Run` phases + injection API + codec identity
   (ops/CLI replay plumbing, CLI `item` gains a codec field).
3. Source committer + quota release fix in `forceTerminal`/`Abandon`.
4. Acceptance tests and docs (`docs/developer/02-engine.md`,
   `01-architecture.md`, `03-plugins.md`, `docs/plugins.md`).

**Acceptance tests.**

1. Crash recovery with more uncommitted rows than a channel holds (e.g. 300
   rows, `ChannelSize` 32) completes — it hangs before the fix.
2. Lock-holding source under backpressure: file source + small channel +
   slow sink completes; the committer is invoked with the final frontier and
   the state is persisted.
3. Refusal contract: `AppendHook` failure → source receives the error →
   source failure → run failed (batch exit 1 / job `JobFailed`); ctx
   cancellation → source returns nil, not a failure.
4. Codec identity: a csv dead letter replays as csv; internal injection can
   carry a non-json codec.
5. Quota invariants: replay acquire/release balances; `Abandon` releases;
   replay never exceeds HighWatermark.
6. Source committer: coalescing (many advances converge to the max frontier),
   failed commit retained and retried on the next advance, shutdown flush
   leaves `SetSourceState` at the latest frontier.
7. Full regression: engine/jobs/cli/rpcplugin/store suites + `-race`;
   examples contract suites green (including the updated ticker-source).
8. Docs and plugin surface: `plugins.md`, the developer guides,
   `pkg/plugin` comments, CHANGELOG (BREAKING: emit signature, injection
   API).

**Risks.** ABI ripple is mechanical but wide (nine sources + adapter +
example module). Source state may lag the checkpoint on crash → duplicate
delivery, never loss (documented). The `Quiesced` guard gap for injected
messages is owned by candidate 02. The deadlock regression test needs a
deterministic construction (small channels + store hooks + deadline).

---

### 3.2 Candidate 02 — One outcome for every runner

**Problem.** `WaitQuiesced` is shared, but every runner re-derives the
terminal state around it: jobs reads a racy `OnSourceError` callback (a
source failure can be observed as quiesced before the callback fires and
reported as success), batch reads `SourceErrors()`, ops polls `Quiesced()`
with a 100 ms settle window and reports a failed batch as still `running`.
The settle constants are copy-pasted (`DrainTimeout+5s`, `+2s`), SIGTERM
maps to exit 1 for batch and 0 for the job scheduler, and `Abandon`
force-terminates messages even when the dead-letter write failed
(`engine.go:831`) — advancing the checkpoint past an undurable record.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | Engine owns `Outcome` + `Wait(ctx, runDone, opts)`; runners only read it | "How did the run end" gets one decision point. |
| 2 | `Abandon(ctx, reason)` is bounded and force-terminates only after a successful dead-letter write; failure leaves messages uncommitted and returns an error | Writing first is the invariant; a failed abandon must not lose the record. |
| 3 | `interrupted` maps by process contract: one-shot runs (batch, trigger) exit non-zero; a long-lived scheduler exits zero on graceful stop | The exit code answers different questions for the two process shapes. |
| 4 | A source failure terminates the engine in every mode | No silent "alive but not consuming"; restart resumes from watermarks. |
| 5 | Delete `Options.OnSourceError`; `SourceErrors()` snapshot (carried by Outcome) is the only channel | One truth source; the jobs race disappears. |
| 6 | Acceptance: 9 items | — |

**Design outline.**

- `RunStatus`: `completed | partial | failed | interrupted`. Classification
  priority: worker-fatal → source errors → caller cancellation → dead
  letters > 0 (partial) → completed. An engine self-stop (source failure)
  is never misreported as interrupted.
- `WaitOptions{AbandonOnCancel, AbandonReason, AbandonTimeout}`: jobs
  abandons on cancel (R2 semantics); batch does not (uncommitted rows replay
  on the next run; dead letters are operator data, not garbage).
- `WaitQuiesced` stays as the low-level primitive (tests use it);
  `Quiesced` gains the `replayDone`/`admitting` guards.
- Consumer mapping: jobs → `JobSuccess/JobPartial/JobFailed/JobCanceled`;
  batch and trigger → 0/1; ops → `completed`/`failed` (interrupted is the
  shutdown's business).

**Implementation steps.** ① engine: Outcome/Wait/Abandon/guards → ②
consumers: `finishBatchRun`, `runOnce`, `watchBatchCompletion`, remove the
callback, source failure terminates → ③ exit-code mapping, docs, tests.

**Acceptance tests.**

1. Outcome matrix: four statuses constructed directly and asserted.
2. Exit codes: batch 0/1/1/1; trigger success=0, partial=1, else 1;
   scheduler SIGTERM=0.
3. Source failure terminates in continuous mode (process exits 1).
4. Bounded Abandon: a failing `WriteDeadLetter` → error, messages stay
   uncommitted (no force-terminate), next run replays them; success path
   advances only after the record lands.
5. Callback removal compiles and behaves.
6. ops status machine: failed batch → `failed` (not running+err);
   completed → `completed`; no 100 ms settle.
7. `Quiesced` guards pinned (injection scenario).
8. Regression: engine/jobs/cli/ops + `-race`; MCP agent-loop.
9. Docs: 02-engine, 01-architecture, 06-observability (status/exit table),
   CHANGELOG.

**Risks.** Continuous liveness changes (multi-source pipelines stop
together; restart resumes). `JobCanceled` must distinguish caller
cancellation from engine self-stop. ops must not report a failed batch as
running.

---

### 3.3 Candidate 04 — One owner for the durable store of a pipeline

**Problem.** `ops.Options.StoreFor` reads like a getter but every
implementation opens a fresh SQLite (schema + migrations + handles) per
call; `Status` calls it per job pipeline per SSE/UI poll and nothing ever
closes those handles. The same concept has three on-disk layouts
(`eventboat.db`, `stores/<name>.db`, `stores/pipeline.db` — the last ignores
the pipeline argument). `memStore`'s spool ignores the pipeline argument the
SQLite adapter keys on, contradicting its own comment. `Store` bundles 24
methods across three unrelated concerns.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | `store.Owner` + injectable provider interface; `ops.Options.StoreFor` deleted | Layout and handle lifetime get one owner; tests inject a memory owner. |
| 2 | Canonical layout `dataDir/stores/<sanitized>.db` for every entry point; old layouts retired (no migration in beta) | Pipeline-scoped interface semantics; fault isolation; one-shot verbs and the daemon read the same file. |
| 3 | Split into `SpoolStore` / `DeadLetterStore` / `JobRunStore` + a combined `Store` | Modules depend only on what they use; fakes shrink. |
| 4 | `memStore` keys by pipeline like SQLite; conformance tests across both implementations | A local-substitutable seam must actually substitute. |
| 5 | Acceptance: 8 items | — |

**Design outline.**

- `Owner` owns path normalization, handle caching (one per pipeline per
  process), name sanitization and the Windows-reserved-name check, and an
  idempotent `Close`. The memory owner caches too — this fixes `--ephemeral`
  cross-surface invisibility.
- Name rules (`Sanitize`, `WindowsReservedName`) move to a shared leaf so
  config and store cannot drift.
- `engine.New` and `jobs.New` narrow their store parameters to the facets
  they need; ops takes the combined interface.
- The store path must be exposed to candidate 08's lease (single writer).

**Implementation steps.** ① Owner + name leaf + layout → ② facet split and
dependency narrowing → ③ memStore alignment + conformance tests → ④
consumers and docs.

**Acceptance tests.**

1. One layout: trigger-written history is readable by the `jobs` CLI and the
   daemon's `Status` (cross-entry consistency test).
2. Handle caching/lifetime: N `Status` polls open one handle; shutdown
   closes all; no leaked handles.
3. Facet split: compile-time assertions for both implementations; engine/jobs
   dependencies narrowed.
4. Conformance: identical operation sequences on SQLite and memory produce
   identical results (spool/checkpoint/source state/dead letters/job runs,
   multi-pipeline isolation).
5. Name rules: one implementation shared by config and store.
6. Old layouts retired: `eventboat.db`/`pipeline.db` no longer read; ops
   default factory deleted; CHANGELOG note.
7. Regression: engine/jobs/ops/cli/store + `-race`; `--ephemeral` green.
8. Docs: 01-architecture package map, 04-config-pipeline, 08-building,
   CHANGELOG.

**Risks.** Mechanical churn of the facet split. `--ephemeral` semantics
change from "new store per call" to "process-cached" (a fix, documented).
Old data files are no longer read (beta ruling).

---

### 3.4 Candidate 05 — One verify-first path

**Problem.** Load → build → merge → severity is composed at about ten call
sites with different policies: `ops.Verify` always builds the IR (agents see
cascade diagnostics the CLI skips), `--strict` exists only in the CLI, the
CLI's `--where` compiles with constants while MCP's does not, `--ids` with
`--limit` deletes dead letters that were never replayed, the LSP's document
URI is dropped so relative paths resolve against the process CWD,
`hasError`-style scans are copy-pasted eight times, and `ops.Deploy` parses
the config two or three times and writes the file before shutting the old
instance down.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | New `internal/verify` package: `File/Bytes → Result{Pipeline, Diags, OK}`, strict verdict included | `ops` cannot host it — `ops` depends on `testrun`/`explain`/`jobs`, so those consumers could never call it. |
| 2 | Unify the strategy surface (verify, explain options, DLQ filter/selection/replay semantics); trigger/jobs/run stay local but share verify + the store owner | Local one-shot execution and daemon control are different things; forcing them together would grow `ops` a "temporary engine" mode. |
| 3 | Explicit `baseDir` on content-based entries | The documented rule "paths resolve relative to the pipeline file" must hold for LSP/MCP/Admin too. |
| 4 | Acceptance: 8 items | — |

**Design outline.**

- `internal/verify` exposes a two-stage form for consumers with a middle
  step: `LoadBytes(name, content, baseDir)` → (jobs substitutes parameters)
  → `Build(p, reg, opts)`; plus the one-shot `File/Bytes`. `Result.OK`
  applies the strict policy once.
- `config.Diagnostics` becomes a first-class value (`HasErrors`,
  `FirstError`, `StrictOK`); the eight hand-rolled scans disappear.
- `internal/dlq` owns filter compilation (with constants), selection order
  (select before delete) and codec-carrying replay requests; the live
  injection transport (`ops`) and the local-engine transport (CLI) both
  construct `InjectReplay(node, registry.Message{...})` from candidate 01.
- `ops` gains a Service-free explain entry with `EntryNode`, so CLI and
  MCP/Admin behave identically.
- `ops.Deploy`: verify in memory → write the file → swap the instance;
  single parse; no half-deployed state.

**Implementation steps.** ① `internal/verify` + `config.Diagnostics` +
call-site migration → ② DLQ/explain/deploy unification → ③ tests and docs.

**Acceptance tests.**

1. Single composition: only `internal/verify` builds pipelines; the same
   input yields the same diagnostic sequence and strict verdict on CLI,
   MCP, Admin and LSP.
2. `config.Diagnostics` methods replace the hand-rolled scans.
3. DLQ semantics: constants-aware filters on both surfaces; `--ids` +
   `--limit` no longer deletes unreplayed rows; selection order consistent;
   codec carried (candidate 01).
4. baseDir: LSP resolves wasm/grpc relative paths against the document
   directory (two same-named files in different directories); empty baseDir
   for MCP documented.
5. Explain options consistent (`--at` everywhere or nowhere).
6. Deploy single-parse, no half-deployed state.
7. Regression: cli/ops/lsp/jobs/testrun + agent-loop + examples; `-race`.
8. Docs: 04-config-pipeline, 01-architecture, 06-observability, CHANGELOG.

**Risks.** Wide call-site migration. jobs' two-stage path must use the same
primitives. The DLQ module must keep the transport difference explicit.

---

### 3.5 Candidate 06 — One framework vocabulary

**Problem.** The framework-field whitelist is hand-copied into five modules
and has already drifted: `grpc` and `version` are node fields in config but
registrable as plugin names, so such a plugin registers and can never load.
`Pipeline.Order` promises declaration order but comes from Go map iteration
(jobs binds the cursor to a random source's watermark; explain picks a
random entry node). Framework defaults are re-expressed at every layer
(`json` ten times). `edge_defaults` silently drops `when`/`route`.
`sinks.workers` is accepted and ignored; `mcp.enable` has no reader;
`runtimecfg` claims pipeline-config strictness but ignores type errors;
`metadata` has no unknown-field check.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | New leaf `internal/framework`: per-section framework fields, top-level keys, edge attributes, reserved names, node-level default constants | Both config and registry keep their documented zero-internal-dependency leaf property. |
| 2 | `Order` is the YAML declaration order (from the node stream, reusing `lineIndex`) | The documented promise becomes true for cursor binding, explain entry and diagnostics. |
| 3 | Pipeline-level defaults materialize at load; engine runtime defaults get one normalization path | Downstream stops re-applying defaults; runtime knobs stay overridable. |
| 4 | `edge_defaults` rejects `when`/`route` with `cfg_edge_defaults_field` | A global default predicate is a footgun; a default route is meaningless. |
| 5 | `sinks.workers` rejected with a diagnostic; `mcp.enable` implemented (daemon gates `/mcp`; `mcp --http` is explicit and documented) | No dead knobs: implement or refuse. |
| 6 | Acceptance: 8 items | — |

**Design outline.**

- `internal/framework` also carries the reserved-name set; the registry's
  registration check and the LSP completion both read it.
- The loader iterates section members in YAML node order; `Pipeline.Order`
  is document order.
- `metadata` gains strict checking; `runtimecfg` moves to a typed struct
  with the shared strict decode (unknown keys and type errors diagnosed).
- `mcp.enable=false` → the daemon does not register `/mcp`.

**Implementation steps.** ① framework package + config/registry/LSP switch
→ ② order + defaults materialization → ③ edge_defaults + dead knobs +
strictness → ④ tests and docs.

**Acceptance tests.**

1. Vocabulary single-source; reserved names == the union of framework fields
   (test-pinned).
2. Declaration order: shuffled YAML yields document order in `Order`.
3. Defaults materialized after load; downstream `if == ""` defaulting
   deleted; one runtime normalization path.
4. `edge_defaults` diagnostics; `delivery/required/buffer` unchanged.
5. Dead knobs: `sinks.workers` diagnostic; `/mcp` gated.
6. Strictness: runtimecfg type errors and unknown keys; `metadata` checked.
7. Regression: config/registry/lsp/ir/jobs + examples + agent-loop; `-race`.
8. Docs: 04-config-pipeline, 03-plugins, 06-observability, 01-architecture,
   CHANGELOG.

**Risks.** Loader node-order rework must preserve line numbers and strict
diagnostics. `sinks.workers` changing from silently ignored to an error is a
behavior change (beta, CHANGELOG). LSP copy and docs must stay in sync.

---

### 3.6 Candidate 07 — Typed failure kinds across the transform seam

**Problem.** The engine classifies transform failures by matching message
text produced in other packages (`strings.Contains(serr.Msg, "too many
steps")`, `Contains(err.Error(), "exceeded")`), so a guest error containing
"exceeded" counts as a wasm timeout. `TransformFlavor` is documented as
feeding metrics but the engine's switch has no default and never records
unknown flavors; `TransformError.Flavor` is a second, unused flavor channel
(split fills it without implementing the interface). `obs.ReasonClass`'s
prefix list is already wrong (`"encode: "` does not match `"encoder"`; every
non-script flavor lands in `other`).

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | Hosts define typed kinds; plugin adapters map them into the registry kind enum | Hosts keep their documented leaf property; the engine sees one enum; no text matching. |
| 2 | Flavor travels only through `TransformFlavor`; delete `TransformError.Flavor`; engine switch gains a default branch; split implements `Flavor()` | Flavor is an instance property (success-path histograms need it), not error data. |
| 3 | Dead letters carry `Class` recorded at production time (transform classes are the failure kinds); `obs.ReasonClass` deleted; `encoder`/`encode` unified | The class is known where the failure happens; deriving it from text is what broke it. |
| 4 | Acceptance: 7 items | — |

**Design outline.**

- `starhost.ScriptError` gains `Kind` (compile/steps/runtime); `wasmhost`
  errors gain `Kind` (compile/timeout/guest/trap); the builtin adapters map
  to `registry.TransformError.Kind` (steps/timeout/guest/compile/runtime/other).
- `TransformError` loses `Flavor` and `Flag`; the engine records by kind and
  by the interface flavor, with a generic default.
- `store.DeadLetter` gains `Class`; metrics use it directly; the
  `reason_class` label values change (CHANGELOG).
- The `wasm: ` prefix stripping goes away (host text preserved).

**Implementation steps.** ① host kinds + adapter mapping → ② flavor single
channel + engine default → ③ dead-letter class + metrics + schema → ④ tests
and docs.

**Acceptance tests.**

1. No string sniffing remains (grep gate) in starhost/wasmhost/builtin/
   engine/obs.
2. Kind matrix per host; adapter mapping tests.
3. Flavor single channel; unknown flavor recorded generically (fake flavor);
   full build after field removal.
4. Dead-letter classes correct on every path; metric labels match;
   `"encode:"` no longer lands in `other`.
5. False-positive regression: a guest error containing "exceeded" is not a
   timeout.
6. Regression: starhost/wasmhost/builtin/engine/obs + wasm examples + `-race`.
7. Docs: 03-plugins, 05-scripting, wasm.md, 06-observability, CHANGELOG.

**Risks.** Dead-letter schema change (per-pipeline DBs; no migration in
beta). Metric label values change (dashboards). Guest errors may still
mention "exceeded"; they are simply typed as guest now.

---

### 3.7 Candidate 08 — The jobs manager owns run admission and terminal transitions

**Problem.** Overlap admission is a TOCTOU check: the lock is released
before registration, so concurrent triggers defeat `overlap: skip`/`latest`.
`Manager.Stop` waits only the scheduler goroutine, so `Drain`/`Pause`/
`Deploy` return while runs are still abandoning; a subsequent `Deploy` can
start a second execution of a still-running run. Concurrent `Deploy` calls
can start two managers on one store. `Resume` after `Drain` is a silent
no-op that reports `{"status":"running"}`. An async `Trigger` drops the run
id and returns `null`. `JobCommitting` is a documented state nothing writes.
`maybeCatchup` materializes an unbounded missed-tick list. And, newly
surfaced by candidate 04: a one-shot verb and the daemon can run the same
pipeline store concurrently — two engines, one spool.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | Atomic admission (admit + register + create run record in one critical section) plus a cross-process store lease (exclusive OS file lock); a one-shot verb that cannot take the lease refuses and points at admin/MCP | One writer per pipeline store; crash releases the lock with the process (no TTL heartbeat). This amends candidate 04. |
| 2 | Manager tracks run goroutines; `Stop` waits for every run to reach and persist its terminal state; `managed.done` means the instance really stopped | Drain must mean drained; the deploy-replay race disappears. |
| 3 | Explicit instance state machine + per-pipeline lifecycle mutex; atomic instance replacement; single status derivation (latest runnable run) | No fake `running`; deploy/drain/pause/resume serialize per pipeline. |
| 4 | Delete `JobCommitting` from the enum, `Runnable()` and docs | A reserved state with no writer must not sit in the contract. |
| 5 | Acceptance: 8 items | — |

**Design outline.**

- `spawn` becomes one critical section; `runAsync` registers the goroutine
  in the manager's wait group.
- Lease: build-tagged file lock (Unix `flock` / Windows `LockFileEx`) on the
  pipeline store file; the daemon holds leases for deployed pipelines;
  one-shot verbs attempt the lock before opening the store.
- `ops.managed` transition table: `running → paused → running`,
  `running → drained → running` (Resume restarts), `completed/failed`
  terminal; per-pipeline mutex serializes lifecycle actions.
- Async `Trigger` returns the run id (admin/MCP response shapes updated).
- `maybeCatchup` computes the last tick inside the window directly.

**Implementation steps.** ① manager atomic admission + run tracking → ②
lease + one-shot verb integration → ③ instance state machine + status/
trigger shapes → ④ catchup, dead state, tests and docs.

**Acceptance tests.**

1. Concurrent triggers with `overlap: skip` admit one; `latest` leaves one
   active; TOCTOU window closed (a controllable store stalls
   `CreateJobRun`).
2. Lease: CLI trigger/replay refused while the daemon holds it; released on
   exit and crash; no residue.
3. Stop/Drain terminal: Stop returns with all runs persisted terminal;
   `RunnableJobRuns` empty after Drain/Pause/Deploy; "deploy while a run is
   unterminal" does not double-run.
4. Transition table tests; Resume from drained restarts; no fake running.
5. Status derivation: latest runnable run; enum/query consistency after
   `JobCommitting` removal.
6. Async trigger returns `run_id` (not null); `wait=true` unchanged;
   agent-loop async case added.
7. Bounded catchup: long outage locates the last in-window tick without
   materializing the list (iteration-count assertion).
8. Regression: jobs/ops/admin/mcpserver/cli + `-race` + agent-loop; docs:
   02-engine, 06-observability, CHANGELOG.

**Risks.** File-lock portability (build tags, testability). Lease-refusal UX
must be clear. The lease is an amendment to 04 and needs its own CHANGELOG
entry.

---

### 3.8 Candidate 09 — Explain renders resolved semantics

**Problem.** The walkthrough claims to match production but re-derives
per-tier knowledge by plugin name: a wasm node without `timeout_ms` shows a
fictional 1000 ms budget (production runs fast mode), a mixed-edge sink
shows the first edge's policy (the engine takes the strictest), optional
edges are shown as dead-lettering (the engine drops), and a non-JSON sample
is decoded as JSON while the output claims the source's decoder. Every edge
is evaluated twice. `ir.Build` retains explain-safe instances for explain
but leaks them on error paths, and the LSP recompiles wasm on every
keystroke.

**Decisions.**

| # | Decision | Rationale |
|---|---|---|
| 1 | Shared resolution + comparison tests: `wasmhost` exposes the resolved mode (fast/budget) used by the adapter and explain; explain renders per-edge policies plus the batch aggregation rule; a matrix test compares explain predictions with real engine terminal states | "Matches production" becomes verifiable instead of asserted; batch aggregation is a runtime property, so explain describes the rule, not a single number. |
| 2 | Verify-only vs `ForExplain`: verify closes instances immediately (including failure paths); explain retains explain-safe instances and closes the pipeline afterwards; wasm compiled artifacts cached by `(path, mtime, cfg)` | Verification needs no retained instances; the LSP keystroke cost and the leak class disappear. |
| 3 | Acceptance: 6 items | — |

**Design outline.**

- explain corrections: wasm mode, per-edge delivery policies + aggregation
  rule, optional drop, sample decoded with the source's decoder, single
  evaluation per edge.
- `ir.Pipeline.Close()`; `verify.Options{ForExplain}`; all failure paths
  close created instances.
- `wasmhost` module cache keyed by file identity and compile-affecting
  config bits.

**Implementation steps.** ① explain corrections + shared wasm resolution →
② lifecycle split + cache → ③ comparison matrix + docs.

**Acceptance tests.**

1. Divergences gone: wasm fast mode; per-edge policies + rule; optional
   drop; decoder-aware sample; single evaluation.
2. Comparison matrix: explain predictions equal engine terminal states for
   when/route, zero-match filtering, optional drop, transform dead letters,
   split expansion.
3. Lifecycle: no retained instances in verify (fake transform Init/Close
   counters); no leak on failure; explain closes; second verify of the same
   file hits the wasm cache.
4. Cache correctness: file change recompiles; cfg change separates keys.
5. Regression: explain/ir/ops/lsp/wasmhost + examples + `-race`.
6. Docs: wasm.md, 04-config-pipeline, 03-plugins (explain-safe + Close
   contract), CHANGELOG.

**Risks.** The comparison matrix needs deterministic scenario construction
(testkit provides it). Cache invalidation correctness. Third-party
explain-safe plugins gain a Close contract (documented).

---

## 4. Global risks

- **Sequencing churn.** 05, 06, 08 and 09 all touch the CLI/ops surface;
  the recommended order (§2) minimizes overlapping rewrites, but the
  implementer should rebase each stage on the previous one rather than
  parallelize across candidates.
- **ABI and schema breaks.** 01 changes the source plugin ABI; 07 changes
  the dead-letter schema and metric label values; 04 changes the on-disk
  layout; 06 turns a silently ignored field into an error. All are allowed
  under the beta ruling; each needs a CHANGELOG entry and updated examples.
- **Behavior changes that operators will notice.** Source failure now stops
  the engine (02); drain waits for runs (08); a one-shot verb may refuse
  when the daemon holds the lease (08). Docs must state each one.
- **Cross-candidate amendments.** 08 amends 04 (lease on the store file);
  09 depends on 06 (materialized defaults) and 05 (`ForExplain`). An
  implementer starting mid-program should read both candidate sections.

## 5. Test strategy

- The eight engine invariants (`internal/engine/invariants_test.go`) remain
  the backbone; candidate 01 adds a replay-capacity regression and candidate
  02 adds the Outcome matrix.
- Each candidate's acceptance list is the completion definition; tests are
  named there.
- Cross-implementation conformance (04) and the explain-vs-engine matrix
  (09) are new test classes; both are deterministic (testkit).
- Gate: `go build ./...`, `go test -race ./...`, golangci-lint clean,
  examples gate green, MCP agent-loop green. `-race` explicitly on
  engine/jobs/cli/ops/store/rpcplugin.

## 6. Explicitly not doing

- **Dropping `Source.Commit`** (state-on-emission) — rejected in 01; the
  per-message cursor model would rewrite the plugin protocol and the hot
  path.
- **A source lock contract** ("never hold a lock across emit") as the fix
  for re-entrancy — overturned in 01 in favor of the source committer.
- **Full CLI delegation to `ops`** — rejected in 05; one-shot execution and
  daemon control stay separate surfaces.
- **`sinks.workers` implementation** — rejected in 06; real sink concurrency
  would redefine batching and `order_key` semantics.
- **Cross-process delegation through the admin API** — not chosen in 08; the
  lease refuses instead (delegation can be layered later).
- **Migrating old on-disk data** (`eventboat.db`, `stores/pipeline.db`) — no
  compatibility obligation in beta; the old files are simply no longer read.
- **An `docs/adr/` directory** — this repo records rulings in the spec's
  revision notes and `CHANGELOG.md`; `CONTEXT.md` carries the vocabulary.
  Introducing ADRs would be a second, competing convention.

## 7. Implementation record

The program was implemented in seven sequential stages, each behind a gate
(full build, the affected packages, `-race`, and the stage's own acceptance
list verified first-hand by the orchestrating session before its restore
point was committed).

| Stage | Candidates | Commit | Gate scope |
|---|---|---|---|
| S1 | 01 | `a5d3160` | full suite · `-race` engine/rpcplugin/cli · examples |
| S2 | 02 | `e14e2a1` | full suite · `-race` engine/jobs/cli/ops · agent-loop |
| S3 | 04 | `3ee0632` | full suite · `-race` store/engine/jobs/ops/cli |
| S4 | 08 | `829bce3` | full suite · `-race` store/jobs/ops/cli/admin |
| S5 | 05 + 06 | `876012d` | full suite · `-race` config/ops/cli/lsp/jobs/engine/store/verify/dlq/framework |
| S6 | 07 | `2513c9e` | full suite · `-race` engine/registry/wasmhost/store/lang/obs |
| S7 | 09 | `dff5e9a` | full suite · `-race` explain/ir/ops/wasmhost/engine/verify |

Final acceptance (2026-09-24): the full suite is green; key paths were walked
end-to-end (`verify` + `explain` on `examples/branching`; the fanin batch
example runs to completion with all five messages committed and exit 0); the
scope was checked against §6 — all eight candidates are implemented and
nothing on the not-doing list was built; every stage's acceptance list was
verified first-hand at its own gate before the restore point landed.

## 8. Index maintenance

`docs/README.md` lists this document. When a candidate is implemented,
update its row here (and the index) to `Implemented` with the PR/commit
reference; when a decision is revisited, record the successor document
rather than deleting this one.
