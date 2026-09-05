---
title: "Engine internals"
order: 2
---

# Engine internals

The engine (`internal/engine`) executes one compiled pipeline against one
store: source admission through a durable spool, in-memory DAG execution with
commit tracking, per-edge delivery retries, dead lettering, checkpointing and
backpressure. The package is three files plus tests: `engine.go` (admission,
persistence, lifecycle), `nodes.go` (transform/sink workers, dead letters),
`commit.go` (the commit frontier). Every reliability property is phrased as
one of the seven invariants (see [Architecture](01-architecture.md)), each
with a dedicated test in `internal/engine/invariants_test.go`.

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
3. **Admission.** A semaphore of `Options.HighWatermark` slots
   (`DefaultHighWatermark = 10_000`). The jobs manager can hand in a *shared*
   `Options.Admission` pool so `limits.max_in_flight` aggregates across
   concurrent `overlap: all` runs instead of multiplying per run.
4. **`Run(ctx)` order matters**: crash replay first (read checkpoint →
   `Store.ReplayFrom` beyond it, re-dispatching each row), then transform
   workers and sink workers, then sources *last* — a fresh source restores its
   persisted state via `Init(state)` when one exists, then `Run`s (or `Pull`s
   for pull sources). Sources last so replayed rows are already registered
   before new emissions race them. `replayDone` flips only after the replay
   scan finishes; `WaitCommit` refuses to read "all committed" before that
   (an unregistered replay row momentarily looks like `outstanding == 0`).

## The accept path

`accept` is the single entry point for source emissions (`engine.go`):

1. **Admission gate.** The caller blocks on `admitSem` — one slot per
   uncommitted message. When the high watermark is reached, sources stop
   being served (backpressure; `eventboat_backpressure_events_total`). A
   blocked accept that races shutdown returns `ctx.Err()` and the message is
   *not* accepted — the source re-emits it later; that is at-least-once.
2. **Stamping.** `message_id` (preserved if the caller supplied one, so
   replays keep identity), `ingest_time`, `source`, plus any
   `Options.MetaStamps` (e.g. `job_run_id`). The source node's decoder names
   the message's codec (`json` default).
3. **Durable spool append.** `Store.AppendSpool` returns the spool sequence.
   On failure the admission slot is returned and the message is *refused* —
   never delivered (invariant 1).
4. **Registration.** `acceptedAt[seq]` records the accept time (commit
   latency), `acquired[seq]` remembers the backpressure slot, and
   `commit.arrived(seq, sourceNode, raw.SrcSeq)` pre-registers exactly one
   outstanding branch and records the source emission for watermark
   tracking. Then `dispatchFrom` decodes at the source entry (decode failure
   = dead letter) and fans out.

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
- **Dead lettering** (`deadLetterMsg`) retries the store write *forever*
  (backoff `Options.DLBackoff` = 500ms) until it succeeds or the engine
  shuts down: a dead letter that cannot be written blocks the commit
  (invariant 4) — degraded, not lossy. The record carries the full original
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
  (Kafka offsets, file offsets, SQL watermarks), which
  `persistCheckpoint` persists via `Store.SetSourceState`.

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
  asynchronous to the tracker lock.
- **Spool retention.** One batched trim per window of *durable* progress:
  when `persistedThrough >= retentionDue`, rows at or below
  `persistedThrough - SpoolRetention` are deleted
  (`Store.DeleteSpoolThrough`) and the next due point moves a full window.
  The cutoff is anchored at `persistedThrough`, **not** the in-memory
  frontier — trimming ahead of what a restart would replay from would turn
  at-least-once into loss while checkpoint writes fail. `DefaultSpoolRetention`
  is 10_000 rows; `storage.spool_retention` in the Runtime config overrides it
  (`internal/runtimecfg`).
- Per-source `Commit` states persist alongside the checkpoint, with their own
  monotonic guard (`srcPersisted`).

## Recovery

Crash recovery is the `Run` prologue: read the checkpoint, replay every
spool row beyond it (`Store.ReplayFrom`), re-dispatch each into the DAG. Rows
whose source node no longer exists in the IR are settled immediately rather
than wedging the contiguous prefix forever. Pull sources resume from their
persisted watermark, so the uncommitted tail may arrive twice: once via
spool replay, once via re-emission — duplicate delivery, never loss
(invariant 3; `TestInvariant_Kill9ReplayReplaysAllUncommitted`).

Job pipelines resume runs found in `pending/running/committing` on startup;
`internal/jobs` watches `Quiesced`/`SourcesDone`/`SourceErrors` to move runs
to terminal states. A canceled run that must stop immediately calls
`Abandon(reason)`: every outstanding message is dead-lettered first (durable
record) and only then force-terminated in the tracker, so the checkpoint
prefix never wedges (review R2).

## Shutdown

Every pipeline runner registers `signal.NotifyContext` for `SIGINT` **and**
`SIGTERM` (the run, config-dir daemon, mcp, replay and trigger verbs —
commit a9250c8 closed the docker-stop gap). On ctx cancellation the engine:

1. stops accepting (`accept` returns ctx.Err()),
2. drains: waits up to `Options.DrainTimeout` (default 10s, overridable by
   `limits.drain_timeout`) for worker and source goroutines, then hard-cancels,
3. flushes pending sink batches (the sink worker's ctx branch flushes before
   returning),
4. closes sinks and transform masters (clones close themselves).

`Run` then returns the fatal error, if any. The ops layer prints the final
status line (the "settle status" report: counts + checkpoint).

## The fatal path

Per-message failures dead-letter. A *node* that cannot run at all is fatal:
`failNode` records the first error (first error wins), cancels the context,
and `Run` returns it after draining — the jobs manager maps a run whose
engine returned an error to `store.JobFailed`. The motivating case is a
transform whose `Clone` fails: the master instance is precisely what the
plugin declared unsafe to share, so the pipeline fails instead of racing
workers on it (`nodes.go`, `runTransform`).

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
