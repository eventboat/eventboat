---
title: "Engine internals"
order: 2
---

# Engine internals

The engine (`internal/engine`) executes one compiled pipeline against one
store: source admission through a durable spool, in-memory DAG execution with
commit tracking, per-edge delivery retries, dead lettering, checkpointing and
backpressure. The package is five files plus tests: `engine.go` (lifecycle,
persistence), `admission.go` (the inbound path), `nodes.go` (transform/sink
workers, dead letters), `commit.go` (the commit frontier), plus
`sourcecommit.go` (the per-source watermark committers). Every reliability
property is phrased as one of the eight invariants (see
[Architecture](01-architecture.md)), each with a dedicated test in
`internal/engine/invariants_test.go`.

## Startup sequence

`engine.New` resolves plugins and allocates structures; `Run` starts them
(`internal/engine/engine.go`):

1. **IR in, plugins instantiated.** For each node in `ir.Pipeline.Order`:
   sources and sinks instantiate through the registry (`reg.NewSource` /
   `reg.NewSink`) or, for `grpc:` blocks, through `rpcplugin.SpawnSource` /
   `SpawnSink`; codecs resolve by name (named `codecs:` declarations come
   pre-instantiated on the IR, bare names instantiate config-less).
   Transforms instantiate through `reg.NewTransform` and are immediately
   `Init`-ed with a `TransformEnv` — constants, parameters, a node-scoped
   logger and the slow-call advisory — before workers exist. A transform
   `Init` failure closes the instance and aborts startup.
2. **Commit tracker.** `newCommitTracker` gets one `srcTracker` per source
   node and the two callbacks: `onCommit` (per-message bookkeeping) and
   `onAdvance` (checkpoint persistence).
3. **Admission and committers.** `newAdmission` builds the inbound path
   (below); one `sourceCommitter` is allocated per source node.
4. **`Run(ctx)` phases, in this order** (candidate 01; the ordering is an
   invariant, not a preference):
   1. **committers + workers** — one goroutine per source committer, then
      the transform and sink workers;
   2. **crash replay** — read the checkpoint and re-dispatch every spool row
      beyond it through admission (`replay` mode). Replay dispatches into
      node channels, so the consumers must already exist: a recovery with
      more uncommitted rows than a channel holds used to block forever on a
      channel nobody read. A cancelled context stops the scan without an
      error (a voluntary stop, v1.24);
   3. **sources last** — a fresh source restores its persisted state via
      `Init(state)` when one exists, then `Run`s (or `Pull`s for pull
      sources). Sources last so replayed rows are already registered before
      new emissions race them. `replayDone` flips only after the replay
      scan finishes; `WaitCommit` refuses to read "all committed" before
      that (an unregistered replay row momentarily looks like
      `outstanding == 0`).

## The admission path

`internal/engine/admission.go` owns how a message enters the DAG: the
backpressure gate, the acquired-slot ledger, stamping, the spool append,
commit registration and the dispatch step. One entry (`admit`) has three
modes that differ only in request fields:

| Mode | Entry | Append | Stamps | Source ref | Codec |
|---|---|---|---|---|---|
| **live** (source emission) | out of the source node | yes | `message_id`, `ingest_time`, `source`, `MetaStamps` | `commit.arrived(seq, node, srcSeq)` | node decoder (json default) |
| **replay** (crash recovery) | as the spooled row demands (`injected_at` → into the node, else out of its source) | no (existing seq) | original, kept | `arrived(seq, "", 0)` | message (spooled), json default |
| **inject** (operator/test) | into the target node — out of it when the target is a source, which has no inbound processing | yes | live stamps + `injected_at` (internal target only) | `arrived(seq, "", 0)` | message, json default |

Live admissions also record an accept time (commit latency) and a sampled
span; replay and inject do not. **Every mode takes admission quota** — one
slot per uncommitted message. The uncommitted set is ≤ `HighWatermark` by
construction, so replay cannot wedge on the gate, and the quota is uniform
across entries. The gate can be a shared pool (`Options.Admission`) so the
jobs manager aggregates `limits.max_in_flight` across concurrent
`overlap: all` runs instead of multiplying per run.

The verdict `admit` returns *is* the source contract's emit error: `nil` =
durably accepted; a ctx error = engine shutdown; anything else = **refusal**
(not durable, never visible, safe to re-emit — see
[Plugin system](03-plugins.md)). A refused live emission returns the slot it
took; a refused injection returns the error to `InjectAt`/`InjectReplay`.

## The accept path

`accept` is the thin live shell over admission (`engine.go`):

1. **Admission gate.** The caller blocks on the gate — one slot per
   uncommitted message. When the high watermark is reached, sources stop
   being served (backpressure; `eventboat_backpressure_events_total`). A
   blocked accept that races shutdown returns `ctx.Err()` and the message is
   *not* accepted — the source may safely re-emit it (at-least-once).
2. **Stamping.** `message_id` (preserved if the caller supplied one, so
   replays keep identity), `ingest_time`, `source`, plus any
   `Options.MetaStamps` (e.g. `job_run_id`). The source node's decoder names
   the message's codec (`json` default).
3. **Durable spool append.** `Store.AppendSpool` returns the spool sequence.
   On failure the admission slot is returned and the message is *refused* —
   never delivered (invariant 1).
4. **Registration.** `acceptedAt[seq]` records the accept time (commit
   latency), the acquired-slot ledger remembers the backpressure slot, and
   `commit.arrived(seq, sourceNode, raw.SrcSeq)` pre-registers exactly one
   outstanding branch and records the source emission for watermark
   tracking. Then `dispatchFrom` decodes at the source entry (decode failure
   = dead letter) and fans out.

## Injection

`InjectAt(node, msg)` and `InjectReplay(node, msg)` take a
`registry.Message` (Raw/Meta/Codec/ID); the message's identity and codec
travel with it. `InjectReplay` stamps `meta.is_replay=true` and
`meta.original_message_id` from `msg.ID`, and preserves the ID — a csv dead
letter replays as csv, never as json. `InjectAt` at a source node fans out of
it (spooled and stamped like a live emission, but not attributable to the
source's watermark); at an internal node it is spooled and enters INTO that
node (transform script, sink batch), which is what makes operator replay
re-execute the failing step.

## Per-edge delivery

Edges are resolved by the IR with defaults from `edge_defaults` and
overrides per edge (`internal/ir/ir.go`): `Required` (default **true**),
`Retries` (default **3**), `Backoff` (`exponential` | `constant`, base
`Options.BackoffBase` = 100ms, exponential doubling capped at 30s),
`TimeoutMs` (per-attempt, `Options.DefaultTimeout` = 30s when unset),
`BufferMax` (per-edge channel surge capacity, default 128).

- **Sinks** batch engine-side (`runSink`, `nodes.go`): `batch.size` and
  `batch.timeout_ms` control flush; `writeBatch` encodes each message,
  evaluates `order_key`, takes the *strictest* retry policy of the edges in a
  mixed batch, and on success commits every instance. After retries are
  exhausted, a `required: false` edge drops the message (counted,
  `eventboat_optional_drops_total`) while a required edge dead letters.
- **Transforms** retry per the *incoming* edge's policy (review R6), then
  dead letter with the plugin's backtrace if any. A transform failure never
  fails the node. Zero outputs filter the message (commit-as-filtered +
  `NoMatch`); N outputs expand the commit accounting by N-1 extra branches —
  the `split` plugin's 1→N contract.
- **Dead lettering** (`deadLetterMsg` → `writeDeadLetter`) retries the store
  write *forever* (backoff `Options.DLBackoff` = 500ms) until it succeeds or
  the engine shuts down: a dead letter that cannot be written blocks the
  commit (invariant 4) — degraded, not lossy. `Abandon` calls the same
  writer with a caller-bounded ctx (candidate 02): a failed write stops the
  attempt and the message stays uncommitted for the next run's replay. The
  record carries the full original
  message, node/edge attribution, reason, backtrace and run id.
- **Delivery on shutdown** is deliberately not committed: an instance that
  could not be queued before `ctx.Done()` stays uncommitted and is replayed
  from the spool on restart (invariant 3).

## The commit frontier

`internal/engine/commit.go` implements the heart of the reliability model:

- `outstanding map[int64]int` counts open execution branches per spool seq;
  `openBranches` is a running sum of positive values so `snapshot()` is O(1)
  (it is polled from the hot path — `WaitCommit`'s 2ms loop, ops status).
- `arrived(seq)` pre-registers one unit; fan-out `add(seq, n-1)` grows it;
  each terminal event calls `done(seq)` (i.e. `add(seq, -1)`).
- `advanceLocked` scans the **contiguous prefix**: from `committedPtr` up
  while entries are zero/closed; the scanned range is committed, and the
  just-committed seqs plus the per-source frontier snapshot become the
  callback payload. Callbacks run *outside* the tracker lock (they do store
  I/O; the lock must not convoy on fsync).
- **Ordered `srcRefs` FIFO.** Each source emission is recorded as
  `(spool seq, node, srcSeq)`. Arrival is near-ordered (each source
  goroutine appends and registers back-to-back), but two sources racing
  through `accept` can invert; `addSrcRef` splices those into sorted
  position, so the sweep can pop a contiguous prefix off the head instead of
  scanning the whole in-flight window per message (the old map scan was
  O(high watermark) per message).
- **Per-source frontiers.** `srcTracker` keeps `arrivedAt`/`committedAt`
  sets per source seq and advances `front` over the contiguous prefix of
  emissions committed *this run*. Replayed spool rows are not attributable to
  a live source emission and never reach the tracker — sources re-pull their
  uncommitted tail instead, which is exactly the duplicate-delivery contract.
  Swept ranges are deleted so the maps stay bounded by the in-flight window.
- **`Commit(ctx, throughSrcSeq)` contract** (registry.Source): the engine
  calls it when the contiguous committed frontier advances, with the highest
  committed srcSeq for that source; the source returns its new durable state
  (Kafka offsets, file offsets, SQL watermarks), which the source committer
  persists via `Store.SetSourceState`.
- **Source committers** (`sourcecommit.go`, candidate 01). One worker per
  source receives coalesced **maximum** frontier advances from
  `persistCheckpoint` and calls `Source.Commit` off the committing goroutine
  — so a source may hold an internal lock across `emit` (the file source does)
  without deadlocking the pipeline. A failed commit or state write keeps its
  pending value and is retried on the next advance; the `srcPersisted`
  monotonic guard lives in the committer. After `drain()`, `Run` waits
  (bounded) for the workers to exit and flushes each pending frontier
  synchronously with `context.WithTimeout(context.Background(),
  DrainTimeout)` — the engine ctx is already cancelled, and a cancelled
  `Commit` would discard the last frontier. A source state that lags the
  checkpoint only widens the crash replay window: duplicate delivery, never
  loss.

## Persistence

`persistCheckpoint` (`engine.go`) runs on the goroutine that advanced the
prefix, *outside* the tracker lock. Concurrent advances can flush out of
order, so monotonic guards under `persistMu` keep everything from
regressing:

- `persistedThrough` — the highest checkpoint actually written
  (`Store.SetCheckpoint`); `CheckpointPtr` mirrors it for status. A failed
  write only widens the replay window; the next advance retries. The flush
  position `flushAttempted` advances even on failure so observers waiting on
  the durability barrier are not wedged by a permanently broken store.
- `durableThrough()` reports `flushAttempted` — the **visibility barrier**.
  `WaitCommit` and `Quiesced` require `durableThrough >= committedThrough`,
  so "committed" still implies "persistence attempted" now that flushing is
  asynchronous to the tracker lock. `Quiesced` additionally refuses to read
  "no work" while the crash-replay scan is in flight (`replayDone`) or while
  an admission holds a gate slot but has not registered yet (`admitting`) —
  both windows would otherwise look like an empty pipeline (candidate 02).
- **Spool retention.** One batched trim per window of *durable* progress:
  when `persistedThrough >= retentionDue`, rows at or below
  `persistedThrough - SpoolRetention` are deleted
  (`Store.DeleteSpoolThrough`) and the next due point moves a full window.
  The cutoff is anchored at `persistedThrough`, **not** the in-memory
  frontier — trimming ahead of what a restart would replay from would turn
  at-least-once into loss while checkpoint writes fail. `DefaultSpoolRetention`
  is 10_000 rows; `storage.spool_retention` in the Runtime config overrides it
  (`internal/runtimecfg`).
- **Dead-letter retention (opt-in).** Rides the same window: a pipeline that
  sets `dlq.retention` sweeps dead letters older than
  `Clock() - DLQRetention` once per retention window
  (`Store.DeleteDeadLettersBefore`, bounded 10,000-row batches, driven by the
  `dead_letter(pipeline, created_at)` index so a large backlog stays a range
  scan). Unset (the
  default) keeps everything forever — dead letters are operator data for
  `replay`, so the engine never deletes what the pipeline did not ask to
  delete; removed rows are gone from `replay` for good. A failed sweep is
  logged and retried by the next window, exactly like the spool trim — it
  never blocks the commit path (deleting terminal artifacts cannot affect
  the invariants).
- Per-source `Commit` states persist through the committers (above), with the
  monotonic guard inside each committer; the checkpoint stays the durable
  barrier.

## Recovery

Crash recovery is `Run`'s second phase: read the checkpoint, replay every
spool row beyond it (`Store.ReplayFrom`) through admission, re-dispatching
each into the DAG — with the workers already running, so a backlog larger
than a node channel cannot wedge. Rows whose source node no longer exists in
the IR are committed immediately rather than wedging the contiguous prefix
forever. Pull sources resume from their persisted watermark, so the
uncommitted tail may arrive twice: once via spool replay, once via
re-emission — duplicate delivery, never loss (invariant 3;
`TestInvariant_Kill9ReplayReplaysAllUncommitted`).

Job pipelines resume runs found in `pending/running/committing` on startup;
`internal/jobs` drives each run through `Engine.Wait` and maps the `Outcome`
onto the run status (below). A canceled run that must stop immediately
dead-letters its outstanding set through the bounded `Abandon(ctx, reason)`:
the durable record is written **first** and only then is the tracker cleared
(force-terminate, which also releases the admission slot the old path
leaked — candidate 01), so the checkpoint prefix never wedges (review R2). A
ctx cancellation or store failure stops the abandon attempt, leaves every
unrecorded message uncommitted and returns an error — the next run replays
them (never loss, candidate 02). The normal dead-letter path keeps invariant
4: it retries forever on the engine ctx; only `Abandon` is bounded.

## Run outcome

One decision point for "how did the run end" (candidate 02): every runner
calls `Engine.Wait(ctx, runDone, WaitOptions)` and maps the returned
`Outcome` onto its own process contract — none of them re-derives the
terminal state.

- **Statuses.** `RunStatus` is `completed | partial | failed | interrupted`:
  completed = quiesced cleanly; partial = quiesced with dead letters; failed
  = the engine stopped itself (worker-fatal or a source failure);
  interrupted = the **caller's** ctx was canceled (never the engine's
  internal cancel, so an engine self-stop is never misreported as
  interrupted).
- **Classification priority** (one `classifyOutcome` switch): worker-fatal →
  source errors → caller cancellation → dead letters > 0 → completed.
- **`Outcome`** carries status, rows read, committed, dead-lettered, the
  `SourceErrors` snapshot, the worker-fatal error, and the abandon count /
  error. `FailureText()` renders the operator-facing cause.
- **`WaitOptions`**: `AbandonOnCancel` (jobs: R2 semantics; batch does not —
  uncommitted rows replay), `AbandonReason`, `AbandonTimeout` (bounded, and
  the caller ctx is already canceled on that path, so Wait derives a fresh
  context) and `DrainTimeout` (the post-cancel wait bound, defaulting to the
  engine's `DrainTimeout + 5s`). Past that bound a non-cancellable writer may
  outlive the run by design; Wait then folds in the engine's published fatal
  (`failNode` records it before it cancels) instead of waiting on `Run`.
- **`WaitQuiesced` stays** the low-level primitive (tests use it); `Wait`
  builds on the same loop.

A source failure (v1.24: `Run`/`Pull` returned a non-nil error) **stops the
engine in every mode**: the error is recorded first, then the engine cancels
— a continuous pipeline must not keep running with a dead source. Restart
resumes from the source watermarks (duplicate delivery, never loss). An
error returned under an already-cancelled engine ctx is a shutdown artifact
and never a source failure. `Options.OnSourceError` was deleted; the
`SourceErrors` snapshot carried by the outcome is the only channel.

## Shutdown

Every pipeline runner registers `signal.NotifyContext` for `SIGINT` **and**
`SIGTERM` (the run, config-dir daemon, mcp, replay and trigger verbs —
commit a9250c8 closed the docker-stop gap). On ctx cancellation the engine:

1. stops accepting (`accept` returns ctx.Err()),
2. drains: waits up to `Options.DrainTimeout` (default 10s, overridable by
   `limits.drain_timeout`) for worker and source goroutines, then hard-cancels,
3. flushes pending sink batches (the sink worker's ctx branch flushes before
   returning),
4. closes sinks and transform masters (clones close themselves),
5. stops the source committers and flushes their pending frontiers with a
   bounded, non-cancelled context (candidate 01).

`Run` then returns the fatal error, if any. The ops layer prints the final
status line (the "settle status" report: counts + checkpoint).

## The fatal path

Per-message failures dead-letter. A *node* that cannot run at all is fatal:
`failNode` records the first error (first error wins), cancels the context,
and `Run` returns it after draining — the run outcome is `failed`. The
motivating case is a transform whose `Clone` fails: the master instance is
precisely what the plugin declared unsafe to share, so the pipeline fails
instead of racing workers on it (`nodes.go`, `runTransform`). A failed
*source* stops the engine the same way without a worker-fatal error: the
error lands in `SourceErrors`, the outcome is `failed`, and `Run` returns nil
(candidate 02).

## Transform workers

`workers` (default 1) spawns that many goroutines per transform node, all
reading the node's channel. A plugin implementing `TransformCloner` is
cloned **once per worker** (`Clone()` after `Init`); stateless plugins share
one instance — script programs are immutable and per-message state lives in
copy-on-write bindings, `split` has no state. A failed `Clone` is
worker-fatal (see above). `TransformFlavor` ("script", "wasm") selects the
per-flavor duration histogram and budget/timeout counters.

## Options and defaults

| Option | Default | Meaning |
|---|---|---|
| `HighWatermark` | `DefaultHighWatermark` = 10_000 | in-flight (uncommitted) cap; `limits.max_in_flight` overrides |
| `SpoolRetention` | `DefaultSpoolRetention` = 10_000 | spool rows kept behind the checkpoint; `storage.spool_retention` overrides |
| `ChannelSize` | 128 | per-node channel capacity (per-edge `buffer.max_events` raises it) |
| `BackoffBase` | 100ms | delivery retry base; exponential doubling, 30s cap |
| `DLBackoff` | 500ms | dead-letter write retry interval |
| `BatchFlush` | 1s | default sink batch flush interval (`batch.timeout_ms` overrides) |
| `DefaultTimeout` | 30s | per sink-write attempt when the edge sets no `timeout_ms` |
| `DrainTimeout` | 10s | graceful drain bound (`limits.drain_timeout` overrides) |
| `WasmSlowCallWarnMs` | 5000 | zero-interference slow-call watchdog threshold |
| `SpanSampleRate` | 0 | per-message spans, opt-in (`telemetry.span_sample_rate`) |
