# Context — Eventboat domain language

The glossary design work in this repo uses. Code, docs and review discussion
should stay on these names. Entries marked **(settled)** were fixed by the
2026-09-23 architecture review; the rest name concepts the engine already
implements.

## Reliability core

**Pipeline** — one three-section YAML document (`sources`, `transforms`,
`sinks`) joined by `depends_on` edges into a DAG, plus framework sections
(`limits`, `run`, `codecs`, `dlq`, `telemetry`, `parameters`, `hooks`).

**Source / Transform / Sink** — the three node kinds; each is a registry
plugin. A source emits messages, a transform maps one message to zero or
more, a sink acknowledges one batch.

**Edge** — a directed connection between nodes carrying an optional
predicate (`when`/`route`) and a delivery policy (`delivery`, `required`,
`buffer`).

**Spool** — the durable append-only record of inbound messages, written
before a message becomes visible to the DAG (invariant 1). The spool is the
replay source after a crash; the checkpoint is the frontier a restart
replays beyond.

**Admission** **(settled)** — the engine module that turns one inbound
message into a spooled, commit-registered, dispatched message. One entry
(`admit`) with three modes:

- **live** — a source emission: stamped (`message_id`, `ingest_time`,
  `source`, `MetaStamps`), appended to the spool, its source-watermark
  reference registered.
- **replay** — a crash-recovery row: already spooled, original stamps kept,
  not attributable to a live emission.
- **inject** — an operator/test message entering a node: stamped with
  `injected_at`, codec carried by the message.

The admission module owns the backpressure gate, the acquired-slot ledger,
stamping, spool append, commit registration and the dispatch step.
*Not*: ingest, accept, entry.

**Refusal** **(settled)** — the engine rejecting an emission because the
spool append failed or the engine is shutting down. A refused message is not
durable and never visible; the source may safely re-emit it. Sources learn
of a refusal through `emit`'s error return; the builtin default is to report
it as a failed source (the source watermark is the no-loss safety net).
*Not*: drop, loss, rejection-with-retry (the engine does not retry).

**Commit frontier** — the contiguous prefix of spool sequences whose every
execution branch reached a terminal state. The frontier advancing is the
only thing that may move the checkpoint.

**Source committer** **(settled)** — the per-source worker that receives
coalesced frontier advances, calls `Source.Commit` with the maximum, and
persists the returned state. It decouples watermark persistence from the
committing goroutine, so a source may hold an internal lock across `emit`
without deadlocking the pipeline. A failed commit keeps its pending
frontier and is retried on the next advance; shutdown flushes after drain.
*Not*: settle worker, offset writer.

**Checkpoint** — the durable frontier a restart replays beyond. Persisted
synchronously by the commit path; a failed write only widens the replay
window (duplicate delivery, never loss).

**Durable barrier** — the visibility rule that "committed" implies
"persistence attempted": `WaitCommit`/`Quiesced` require the flush attempt
to have reached the committed frontier, even though flushing runs outside
the tracker lock.

**Dead letter** — the durable record of a message that terminally failed
(after retries, or a decode/encode/codec error). Writing a dead letter
retries until success or shutdown (invariant 4): dead-letter unavailability
slows the pipeline instead of losing messages.

**Quiesce** — the pipeline state where every source stopped, nothing is
uncommitted, and every commit advance has been flushed (attempted). Job and
batch runs use quiescence as their completion point; `WaitQuiesced` is the
shared primitive.

**Run modes** — `continuous` (runs until stopped), `job` (one bounded run
per trigger/schedule), `batch` (a continuous-shape pipeline that exits by
itself once quiesced).

**Run outcome** **(settled)** — the terminal classification of one run,
produced by the engine (`Wait`): `completed`, `partial`, `failed`,
`interrupted`, together with counts, source errors and abandoned rows. It is
the single decision point for "how did the run end"; callers map it onto job
statuses and exit codes.

**Partial** **(settled)** — a run that quiesced cleanly but dead-lettered at
least one message.

**Interrupted** **(settled)** — the caller canceled the run. Exit-code
mapping follows the process contract: a one-shot run exits non-zero (it did
not complete); a long-lived scheduler exits zero on graceful stop.

**Abandon** **(settled)** — the bounded, cancellation-time dead-lettering of
outstanding messages: the durable record is written first and only then is
the tracker cleared; a failed write leaves the messages uncommitted for the
next run's replay.

**Store owner** **(settled)** — the module that decides where a pipeline's
durable store lives (`dataDir/stores/<name>.db`) and owns its handle
lifetime: one handle per pipeline per process, cached, closed on shutdown.
Callers depend on the injectable provider interface, not the layout; the
memory owner backs tests and `--ephemeral`.

**Store facets** **(settled)** — the persistence surface is split by
concern: spool/checkpoint/source state (`SpoolStore`), dead letters
(`DeadLetterStore`), job run history (`JobRunStore`). Modules depend only on
the facets they use; both implementations satisfy all three.

**Verify** **(settled)** — the single load → build → judge composition,
owned by `internal/verify`: every surface (CLI, MCP, Admin, LSP, jobs,
testrun, explain) calls it, and no second composition exists. Content-based
entries pass an explicit base directory for relative paths.
*Not*: validation, linting.

**Diagnostics verdict** **(settled)** — `config.Diagnostics` as a
first-class value: `HasErrors`, `FirstError`, `StrictOK`. Callers stop
scanning severities by hand.

**DLQ semantics** **(settled)** — filter compilation (with constants),
selection order (select before delete), and codec-carrying replay requests
shared by every dead-letter surface; the live-injection and local-engine
replay transports sit behind it.

**Framework vocabulary** **(settled)** — the single source of the framework
surface: per-section framework fields, top-level keys, edge attributes,
reserved plugin names and node-level defaults. It lives in the leaf package
`internal/framework`, imported by config, registry and the LSP; no copy of
the lists exists elsewhere.

**Declaration order** **(settled)** — `Pipeline.Order` is the YAML document
order of node declarations, preserved by the loader from the node stream and
consumed by the IR, explain, jobs and diagnostics alike.

**Failure kind** **(settled)** — a typed classification of a transform
failure: `steps`, `timeout`, `guest`, `compile`, `runtime`, `other`. Hosts
(Starlark, wasm) define their own kinds; the plugin adapters map them into
the registry kind the engine consumes. Classification is never derived by
matching error text.

**Flavor** **(settled)** — the per-instance transform family (`script`,
`wasm`, ...) read through the `TransformFlavor` interface; unknown flavors
are recorded generically. Flavor is not carried on errors.

**Dead-letter class** **(settled)** — the coarse failure class recorded
together with the dead letter when it is produced (`decode`, `codec`,
`encode`, `delivery`, the transform failure kinds, `canceled`). Metrics and
operators read the class; it is never re-derived from the reason text.

**Run admission** **(settled)** — the atomic decision and registration of a
job run: overlap policy applied, run record created and the run registered
in one critical section, so `overlap: skip`/`latest` cannot be defeated by
concurrent triggers.

**Store lease** **(settled)** — the exclusive OS file lock a running engine
holds on a pipeline's store. It guarantees one writer per pipeline store
across processes; a one-shot verb that cannot take the lease refuses and
points at the admin/MCP surface. Crash releases the lock with the process —
no TTL heartbeat.

**Instance state machine** **(settled)** — the daemon's per-pipeline
lifecycle (`running`, `paused`, `drained`, `completed`, `failed`) with
explicit transitions, serialized by a per-pipeline lifecycle mutex; a deploy
replaces the instance atomically, and `resume` from `drained` restarts it.

**Explain-safe** **(settled)** — the transform capability declaring that
`explain` may dry-run the instance. A verify-only build validates and closes
instances immediately; an explain build (`ForExplain`) retains explain-safe
instances and closes the pipeline when done.

## Names that carry two senses

- **replay** — (a) *crash replay*: re-dispatching spooled rows beyond the
  checkpoint on `Run`; (b) *operator replay*: re-injecting dead letters
  (`replay --dlq`, MCP/Admin `dlq_replay`). Both enter the DAG through
  admission, in replay/inject mode respectively.
- **commit** — (a) the *commit frontier* advancing in the tracker; (b)
  `Source.Commit`, the source's watermark callback invoked by the source
  committer. The source callback does not move the checkpoint; it only
  persists the source's own durable position.
