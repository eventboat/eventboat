# Log collection mode — VictoriaLogs ingest, file-tail hardening and the tuning surface

| 状态 Status | 日期 Date | 关联 Links |
|---|---|---|
| Draft — P0–P4 implemented | 2026-09-24 | [Architecture deepening](../design/2026-09-23-architecture-deepening.md) (batch-flush direction, §R-B1) · [`competitor-research.md`](../../competitor-research.md) §4 (Fluentd / Fluent Bit) · [Kubernetes deployment](../k8s.md) · [`scripts/bench-gate.sh`](../../scripts/bench-gate.sh) |

This document is the design of record for using Eventboat as the log
collector in a file-based collection scenario — host files and container logs,
mixed Kubernetes and VM estates, mixed single-line and multiline payloads —
with **VictoriaLogs as the only destination**. The driving constraint is a
product one: **no second selection** (no Fluent Bit, no Filebeat). Everything
below is derived from that constraint plus measurements taken on the
reference dev machine (2026-09-24).

---

## 1. Background and current state

### 1.1 What exists already

- `file` source: single-path tail, byte-offset watermark persisted through
  `Source.Commit`, `start_at: beginning|end`, `on_eof: tail|stop`
  (`internal/registry/builtin/file_source.go`).
- Sinks: `kafka`, `http`, `file`, `drop`, `debug`. `Sink.Write(ctx, msgs)`
  is **batch-native** — the engine hands the whole batch to one call.
- Engine: per-message SQLite spool before visibility, commit tracker,
  checkpoint, crash replay, dead letters, per-edge delivery policies,
  backpressure through the admission gate (`docs/developer/02-engine.md`).
- Config: typed structs → generated JSON Schema → `verify`, LSP completion,
  MCP catalog; strict loader (unknown keys are errors); `${VAR}` substitution.

### 1.2 Measured baseline (reference dev machine, 2026-09-24)

All numbers from `go test -bench` on the i5-14600KF dev machine, Windows,
Go 1.25 (`-p 1`), synthetic 4-way fan-out pipeline unless noted.

| Path | Measurement | Implication |
|---|---|---|
| CEL predicate | 766 ns/op | hot-path predicates are cheap |
| Starlark simple script | 1 645 ns/op | transformations are not the bottleneck |
| Engine accept→commit, memory store | 6 621 ns/op ≈ 151 K msg/s | the bookkeeping path has headroom |
| `Store.AppendSpool` (real SQLite) | 51–77 µs/op | **per-message durable write is the ceiling** |
| `SetCheckpoint` / `SetSourceState` | ~30 µs / ~55 µs | one checkpoint write per message today |
| Engine accept→commit, real SQLite, `batch.size=1` | ~926 µs/op ≈ 1.1 K msg/s | writer contention doubles the cost |
| … `batch.size=100` | ~233 µs/op ≈ 4.3 K msg/s | batching helps, but checkpoints still advance per message |
| … same + serialized store writes | ~182 µs/op ≈ 5.5 K msg/s | writer-writer contention is real |
| `BenchmarkCommitThroughput/fsync_sim` (100 µs simulated flush) | 550 795 ns/op ≈ 1.8 K msg/s | the repo's own synthetic durable regime |

A busy host emits 5–50 K lines/s. **The durable path must get an order of
magnitude faster before tail correctness even matters** (§2.2, P1).

### 1.3 The gaps

| # | Gap | Consequence today |
|---|---|---|
| 1 | No VictoriaLogs sink (`http` sink POSTs one message at a time) | cannot ship logs batch-wise |
| 2 | Per-message durable write + per-advance checkpoint write | durable throughput below a busy host's log rate (§1.2) |
| 3 | No rotation/truncation detection: the source reopens only on read error | rename+recreate leaves it on the old fd; copytruncate wedges it silently |
| 4 | No glob, no per-file state | cannot tail `/var/log/containers/*.log` or a directory |
| 5 | No multiline aggregation | Java stack traces become N messages |
| 6 | No file metadata (`file_path`, k8s pod/namespace/container) | no stream fields for VictoriaLogs |
| 7 | No line-size bound | VL silently skips lines > `-insert.maxLineSizeBytes` (256 KiB default) |
| 8 | Deployment shape is a single Deployment + PVC | no DaemonSet/systemd templates; offsets must survive pod restarts |

---

## 2. Target design

### 2.1 VictoriaLogs sink (`victorialogs`)

A dedicated sink plugin, not a generic-`http` configuration. One batch → one
`POST /insert/jsonline` request whose body is the batch's encoded JSON, one
object per line.

| Config key | Default | Maps to |
|---|---|---|
| `url` | — | base URL; the sink appends `/insert/jsonline` |
| `stream_fields` | — | `_stream_fields` (recommended: `host,app` plus `namespace,pod` in k8s) |
| `time_field` | unset (VL's `_time`) | `_time_field` (comma-separated list allowed); unset omits the parameter so VictoriaLogs applies its own default |
| `msg_field` | — | `_msg_field` |
| `extra_fields` | — | `_extra_fields` |
| `gzip` | `false` | `Content-Encoding: gzip` |
| `max_idle_conns` | `2` | transport connection reuse (one sink goroutine by design) |
| `timeout_ms` | `10000` | request timeout |
| `account_id` / `project_id` | — | tenant headers |

**Error mapping** (VL semantics verified against the docs and issues):

- 2xx → batch committed. Note VL **skips invalid lines server-side and still
  returns 200**; this is monitored on the VL side via
  `vl_http_errors_total`, and prevented on ours by guaranteeing the body is
  codec-encoded JSON (§2.3, D8).
- 4xx (all lines unparseable in VL ≥ v1.22) → permanent failure: the edge
  retry policy exhausts and the batch dead-letters; re-sending cannot help.
- 5xx / network error / timeout → transient: retry per edge policy, then
  dead-letter. VL overload applies **pushback rather than dropping**; the
  blocked sink write is what drives engine backpressure (§2.5, D9).

### 2.2 Throughput: group commit in the store (P1, prerequisite)

`Store.AppendSpool` becomes a **group-commit** write: one writer goroutine per
store collects requests and executes one multi-row transaction per batch
(`storage.write_batch.max_rows`, `storage.write_batch.max_wait_ms`), assigning
contiguous `seq` values (`spool.seq` is `AUTOINCREMENT`; `last_insert_rowid -
n + 1`). `SetCheckpoint` and `SetSourceState` coalesce the same way (idempotent
overwrite / monotonic max). Each caller still blocks until its batch has
committed, so **invariant 1 (spool before visible) and the refusal contract
hold unchanged**. A dedicated single writer also removes the writer-writer
`SQLITE_BUSY` contention measured in §1.2.

**Implementation (P1, 2026-09-25).** `internal/store/writer.go`: every mutation
runs on one writer goroutine over one dedicated connection, reads stay on the
pool; the spool group is one multi-row `INSERT`; same-key checkpoints/source
states fold to one upsert each per group behind monotonic high-water marks;
`Close` refuses queued and in-flight writes with `ErrClosed` and always wakes
every waiter. `max_wait_ms` is realised as a bounded scheduler-yield probe
budget, **not** a sleeping timer: an OS-timer floor (Windows: >500 µs) would
cap a lone producer at ~1K rows/s without buying batch companions — see "No
artificial linger" in [`docs/developer/02-engine.md`](../developer/02-engine.md).
Batch sizes are observable through the optional `WriteOptions.OnBatch` hook;
`AppendSpool` keeps its signature and gains no cancellation path, so a
cancelled source still registers a committed row (the orphan-row rule above is
structural, not a callback convention).

One hazard must be handled explicitly: **a waiter cancelled after its row was
batched**. If the caller returned `ctx.Err()` without registering the row, the
orphan row would wedge the contiguous commit prefix until restart. Rule: once
a row is committed by the store, admission completes registration and dispatch
regardless of caller cancellation; only an append *failure* is a refusal.

### 2.3 File source v2 (P2)

| Capability | Design |
|---|---|
| Glob | `path` accepts a glob (`*`/`?`/`[...]`); per-file state, versioned JSON: `{"version":2,"files":{"<id>":{"path":"...","offset":N}}}` (v1 `{"offset":N}` is still applied to a meta-free single path) |
| Identity | `(device, inode)` on Unix, volume serial number + file index on Windows (path fallback on other platforms); the symlink path is kept for metadata, the target for identity |
| Rotation | Same path, new identity → finish the old fd to EOF (emit what remains), then open the new file from `start_at` |
| Truncation | Same identity, `size < offset` → reset to 0 and continue (copytruncate; duplicates are acceptable) |
| Deleted files | Finish the held fd, then drop state (`clean_removed`) |
| fd management | `close_inactive_ms` (default 5m), `ignore_older_ms` (default 0), scan interval `poll_every_ms` (default 250 — the pre-v2 name is kept; it covers the glob rescan and the per-file poll) |
| Line bound | `max_line_bytes` (default 256 KiB, aligned to VL's default), `oversize: truncate \| skip` — truncate emits the first N bytes (the decode/DLQ path makes it observable), skip drops the line and counts it; never a silent drop, and the read buffer is capped at `max_line_bytes` |
| Metadata | Each message carries `meta.file_path`, `meta.host` (P2); container paths (`/var/log/containers/<pod>_<namespace>_<container>-<id>.log`) are parsed into `meta.namespace/pod/container` without needing the k8s API (P3) |

### 2.4 Multiline (P3)

`multiline: { pattern, negate, match: after|before, max_lines, max_bytes,
timeout_ms }` per file source (defaults 500 lines / 1 MiB / 2s; an explicit
`timeout_ms: 0` means no time-based flush and is what
`lint_multiline_no_timeout` warns about). Aggregation flushes on: a new
group-starting line, `timeout_ms`, `max_lines`/`max_bytes`, rotation or
truncation, and stop-mode EOF — deliberately **not on shutdown**: a group that
was not flushed was not committed, so a restart re-reads it from the persisted
watermark (at-least-once, never loss). The watermark for a group is the **end
offset of its last line**; a crash between aggregation and commit re-reads the
group (duplicates, never loss). Lines are joined with `\n`; a group made only
of whitespace is dropped at flush; a line dropped by `oversize: skip` does not
affect grouping, a `truncate` line joins the group truncated. Line/byte caps
bound memory; exceeding them flushes the current group early and starts a new
one with the incoming line.

### 2.5 Deployment shapes

- **Kubernetes DaemonSet** (one pod per node): `hostPath /var/log` read-only,
  **`hostPath /var/lib/eventboat`** for SQLite (offsets must survive pod
  restarts — `emptyDir` is wrong), pipeline directory from a ConfigMap,
  Runtime config pointing `storage.data_dir` at the hostPath. The single
  writer lease works on local disk; DaemonSet pods are independent, so the
  single-active model is not violated.
- **VMs**: systemd unit + logrotate guidance (`create` mode preferred;
  copytruncate is handled but re-reads, so duplicates are expected).
- Mixed estates: `${VAR}` substitution (hostname, app name, paths) and
  per-role pipeline files under `--config-dir`.

### 2.6 Performance tuning surface

Two layers, deliberately split the way the config system already splits
deployment from pipeline: **Runtime** (`kind: Runtime`) for storage and
telemetry, **pipeline** for per-pipeline semantics. Principles:

1. **Defaults first.** The common case must be fast with zero configuration
   (group commit, sink batching are default-on). A knob exists only where a
   genuine tradeoff is user-visible.
2. **Every knob has a metric** that tells you when to change it, and a
   documented direction (§2.6.3).
3. **Guardrails accompany knobs**: verify lints for pipeline knobs, load-time
   validation for Runtime knobs, an effective-values echo at startup.

#### 2.6.1 Existing knobs

| Knob | Layer | Default | Effect | Tune when |
|---|---|---|---|---|
| `limits.max_in_flight` | pipeline | 10 000 | uncommitted cap = backpressure depth = **sink-outage buffer (rows)** | size to `peak rows/s × tolerated outage seconds` (§2.6.2) |
| `limits.drain_timeout` | pipeline | 10s | shutdown wait for workers/sources | large batches or slow sinks |
| sink `batch.size` / `batch.timeout_ms` | pipeline | 1 / 1s | rows per `Write` call / max wait | collection: 500–1000 / 1–5s |
| transform `workers` | pipeline | 1 | per-node concurrency | CPU-bound scripts |
| edge `buffer.max_events` | pipeline | 128 | channel surge capacity | burst smoothing (not a reliability mechanism) |
| edge `delivery.retries` / `backoff` / `timeout_ms` | pipeline | 3 / exponential 100ms / 30s | retry policy | remote sinks |
| edge `required` | pipeline | true | optional-edge isolation | audit/best-effort branches |
| `storage.spool_retention` | Runtime | 10 000 | crash-replay window (rows behind the checkpoint) | raise for slow restarts |
| pipeline `telemetry.span_sample_rate` | pipeline | 0 | per-message spans | keep 0 at collection rates |
| Runtime `telemetry.sample_ratio` / `prometheus` | Runtime | 0.1 / true | trace sampling / metrics listener | set sampling to 0 under load |

#### 2.6.2 New knobs (this program)

| Knob | Layer | Default | Tradeoff |
|---|---|---|---|
| `storage.write_batch.max_rows` | Runtime | 256 | group-commit batch size; larger = higher throughput, coarser latency |
| `storage.write_batch.max_wait_ms` | Runtime | 2 | companion-probe budget before flushing a partial batch; `0` = write-through. Implemented as bounded scheduler yields, not a sleeping timer (P1 note, §2.2) |
| `storage.checkpoint_interval_ms` | Runtime | 0 (every advance) | **trimmed from P1** (2026-09-25): the engine's `persistMu` already serializes checkpoint writes, and delaying them by an interval makes `SetCheckpoint` non-blocking, weakening the `durableThrough` visibility barrier — it needs engine-side changes (see `docs/developer/02-engine.md`, Persistence), so it ships in a later stage if at all |
| file `poll_every_ms` / `close_inactive_ms` / `ignore_older_ms` | pipeline | 250 / 5m / 0 | fd + scan cost ↔ discovery latency (the scan interval keeps its pre-v2 name `poll_every_ms`) |
| file `max_line_bytes` / `oversize` | pipeline | 256 KiB / truncate | memory ↔ oversized-line policy (`truncate` emits the first N bytes and the decode/DLQ path records it; `skip` drops and counts; the read buffer is capped at `max_line_bytes`) |
| file `multiline.{max_lines,max_bytes,timeout_ms}` | pipeline | 500 / 1 MiB / 2s | aggregation memory ↔ group completeness |
| transform `script.max_steps` | pipeline | 100 000 (today hard-coded) | per-message CPU bound ↔ script complexity |
| `victorialogs.{gzip,max_idle_conns,timeout_ms,stream_fields,time_field,msg_field,extra_fields,account_id,project_id}` | pipeline | §2.1 | network/CPU ↔ ingest semantics |

**Sizing rules.**

- **Outage buffer**: `limits.max_in_flight ≈ peak rows/s × tolerated VL
  outage seconds`. At 5 K rows/s the 10 K default absorbs **2 seconds** — this
  is the first parameter to raise in production. Cost is SQLite disk plus
  commit-tracker memory (~100–200 B per uncommitted row: `outstanding`,
  `srcRefs`, `acceptedAt` maps); 100 K rows ≈ tens of MB, millions need
  arithmetic.
- **Disk**: spool rows × (raw + meta) × ~2–3 SQLite overhead. `spool_retention`
  bounds steady-state history, **not** outage buffering — during a sink outage
  the checkpoint stalls and nothing is trimmed.
- **VL throughput**: `batch.size` is the lever (one POST per batch). A sink
  has exactly one goroutine by design (candidate 06 removed `sinks.workers` as
  a dead knob); re-introduce concurrency only with benchmark evidence that a
  single batch-POST worker saturates.
- **Replay window**: `checkpoint_interval_ms × peak rows/s` ≈ extra duplicate
  rows replayed after a hard crash. Duplicates, never loss.
- **Line bound**: keep `max_line_bytes ≤` VL's `-insert.maxLineSizeBytes`
  (256 KiB default), otherwise VL skips the line silently and only its
  `vl_http_errors_total` counter shows it.

#### 2.6.3 Guardrails

- **Verify lints** (pipeline knobs; warnings, `--strict` escalates):
  - `lint_line_bytes_over_vl` — `max_line_bytes > 262144` with a
    `victorialogs` sink: VL will skip longer lines.
  - `lint_multiline_no_timeout` — multiline with an **explicit**
    `timeout_ms: 0`: no time-based flush, so an isolated trailing group waits
    for the next group-starting line (the 2s default is not a problem).
  - `lint_collector_batch_one` — file source + sink `batch.size: 1`: the
    collection shape wants batching.
- **Runtime load validation**: non-negative integers, `max_wait_ms ≥ 0`,
  helpful hint text; a value that is syntactically valid but operationally
  dangerous gets a warning in the effective-values line at startup
  (`storage: batch 256 rows / 2 ms, checkpoint every advance`).
- **Metrics → knob map** (the tuning doc's decision table): file rows read /
  skipped / truncated, rotations, multiline merges, spool append latency,
  commit latency histogram, backpressure events, sink batch size and latency,
  DLQ by class, VL 4xx/5xx.
- **Not exposed** (deliberately): SQLite pragmas (WAL + `synchronous=NORMAL`
  is a documented durability contract), GC (`GOGC`/`GOMEMLIMIT` belong in the
  deployment manifest, documented as such), internal pool sizes, channel
  internals beyond `buffer.max_events`, sink worker counts.
- **Evidence discipline**: `scripts/bench-collect.sh` (source → VL-shaped
  sink, configurable line size and rate) so operators tune on their own
  hardware; the CI `bench-gate.sh` remains a loose regression gate.
  **Landed (P4, 2026-09-25):** `BenchmarkCollectE2E` and the script are in the
  tree (one process per shape — the S2 sensitivity), the reference numbers
  live in both the script header and `docs/tuning.md`, which is the tuning
  surface's operator page.

---

## 3. Key decisions and rationale

| # | Decision | Rationale |
|---|---|---|
| D1 | Eventboat covers the collection scenario; no second selection | Product constraint, confirmed. Cost acknowledged: tail robustness is years of edge cases in Filebeat/Fluent Bit; §4 front-loads it, and the engine's spool/DLQ/replay is the compensating guarantee those tools do not have by default. |
| D2 | A dedicated `victorialogs` sink, not a generic `http` config | Batch JSONL is the only efficient VL ingress; VL's partial-success semantics need sink-level handling; `http` POSTs per message. |
| D3 | Group commit (P1) instead of a `buffered`/lightweight durability mode | A memory spool would void invariants 1–4 (spool-before-visible, checkpoint-over-committed, crash-replay, DLQ-blocks-commit) and fracture the product's "duplicate, never loss" contract. The 2026-09-23 review already named batch flush as the throughput direction (§R-B1). The store interface is the seam; no engine changes needed. |
| D4 | Memory pooling is scoped to raw/scratch buffers; decoded maps are excluded | Sink encoder scratch, spool row marshalling and file read buffers are safe to reuse if ownership is tracked to the terminal state (commit/DLQ). `Decoded`/`Meta` are shared across fan-out branches (invariant 8) and can be retained by DLQ records, Starlark COW bindings, wasm/gRPC serialization — pooling them needs per-message lifetimes the code does not track. Expected win: 10–30 % on the in-memory path, near zero on the durable path; it is not the collection lever. |
| D5 | Metadata travels as `meta.*`; a declarative `fields` transform copies what the pipeline wants into the payload; the sink never merges meta | The engine deliberately encodes `Decoded` only. Sink-side merging would make the sink parse/rewrite payloads — a codec concern. `fields` (static + meta passthrough, `${VAR}` substitution) removes Starlark boilerplate for the common case. |
| D6 | Rotation identity is `(device, inode)`, symlink path kept for metadata | Survives rename+recreate and copytruncate; matches how container logs are laid out; no k8s API needed for pod/namespace/container. |
| D7 | Multiline watermark = end offset of the last line in the group; flush on new group/timeout/caps/rotation/truncation/stop-EOF, **not** on shutdown | Keeps the existing pending-offset machinery; a crash or a shutdown re-reads the uncommitted group (duplicates, never loss) — flushing at shutdown would commit a group the file could still extend. |
| D8 | Oversized lines: bounded and counted, never silently dropped | VL's own default silently skips > 256 KiB; our `oversize: truncate\|skip` makes the choice explicit and observable (truncate emits the first N bytes and the decode/DLQ path records it; skip drops and counts). |
| D9 | `max_in_flight` is the outage buffer; `spool_retention` is not | During an outage the checkpoint stalls, so retention never trims; the admission gate is what bounds the backlog. Sizing formula in §2.6.2. |
| D10 | Knobs default-on where they matter; expose only genuine tradeoffs, each with a metric and a guardrail | Keeps tuning an operator activity, not a prerequisite; the typed-config/registry machinery makes each knob schema-validated, LSP-completed and verifiable at near-zero cost. |

---

## 4. Staged implementation plan

Each stage compiles and keeps the suite green alone; one logical change per
commit; `CHANGELOG.md` entries at commit time (per repo convention).

| Stage | Scope | Acceptance gate |
|---|---|---|
| **P0** | `victorialogs` sink (batch JSONL, params, tenant, gzip, error mapping); collection example pipeline; DaemonSet + systemd templates; `docs/collector.md` | unit tests for body/params/error mapping; `eventboat test` contract case; env-gated integration test against a real VictoriaLogs (query back via `/select/logsql/query`) |
| **P1** | group commit: single writer, multi-row `AppendSpool`, coalesced `SetCheckpoint`/`SetSourceState`; `storage.write_batch.*`, `storage.checkpoint_interval_ms`; append/commit metrics | eight invariants green unchanged; orphan-row-on-cancel test; kill -9 mid-batch leaves no unregistered rows; durable end-to-end ≥ 20 K rows/s on the reference machine (recorded) |
| **P2** | file source v2: glob, inode identity, rotation/truncation, symlinks, per-file state, `close_inactive`/`ignore_older`, `max_line_bytes`/`oversize`, `file_path`/host metadata; `fields` transform | scenario matrix green: kill -9 / rotation / copytruncate / symlink / deleted-file; restart resumes per file; state format v1→v2 compat test |
| **P3** | multiline aggregation; container-path metadata; verify lints (§2.6.3); collection metrics | stack-trace aggregation test; lint tests; metrics asserted against `/metrics` |
| **P4** | `docs/tuning.md` (symptom → metric → knob table, sizing formulas); dashboard/alert notes (incl. `vl_http_errors_total`); `scripts/bench-collect.sh` | doc index updated; bench script runs and prints reference numbers |

Order rationale: P0 makes the destination reachable; P1 is the prerequisite
for any real log rate; P2/P3 are the correctness work; P4 makes the tuning
surface usable.

---

## 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Tail edge cases (rotation races, inode reuse, partial lines, multiline across rotation) are where Fluent Bit/Filebeat spent years | The P2 scenario matrix is the acceptance contract for correctness (no line loss; duplicates allowed); the spool + DLQ + replay is the safety net the competitors lack; document exact semantics rather than chasing parity. |
| Files rotated away before they are read | Keep the fd until EOF after rotation (Unix keeps the inode alive); document logrotate retention (`maxsize`, delete policy) needed for pathological rates. |
| VL returns 200 while skipping invalid lines | Bodies are codec-encoded JSON (structurally valid); `max_line_bytes` aligned with VL's limit; monitor `vl_http_errors_total` and alert. |
| Duplicates (at-least-once, crash replay, copytruncate re-reads) | Documented contract; VL stores duplicates — operations dashboards should expect them after restarts. No dedup in this program. |
| Group-commit latency and orphan rows | Bounded wait (default 2 ms); cancellation rule in §2.2; explicit tests. |
| Single-writer lease on network filesystems | hostPath/local disk or block-backed PVC; documented in `docs/k8s.md`. |
| Knob proliferation | §2.6 principles: defaults first, metric per knob, guardrails, explicit not-exposed list. |
| Scope creep toward a general-purpose agent | §7 lists what this program is not; anything else is a new design document. |

---

## 6. Test strategy

- **Unit**: VL sink (batch body, params, gzip, tenant, 4xx/5xx mapping);
  file source (rotation, truncation, symlink, glob, line bound); group commit
  (batch boundaries, cancellation, crash); multiline aggregation.
- **Contract tests** (`eventboat test`): the collection example pipeline with
  injected fixture lines, asserting captured VL batches.
- **Invariant tests**: `TestInvariant_*` untouched and green after P1 (the
  proof that durability semantics did not move).
- **Scenario matrix** (P2): kill -9 mid-tail; rename+recreate; copytruncate;
  symlink target replacement; file deleted after rotation — assert
  line-count conservation (duplicates allowed, loss not).
- **Integration**: env-gated VictoriaLogs test mirroring the Kafka inttest
  (`internal/inttests/`), asserting ingested lines via LogsQL.
- **Performance**: store write-batch benchmark added to `bench-gate.sh`
  (loose limits); `scripts/bench-collect.sh` for on-prem size tuning; soak
  test extended with a file-source shape under transient sink faults.

---

## 7. Explicitly not doing

- **No general-purpose agent features**: no Windows Event Log, journald,
  syslog receiver, Kubernetes API metadata enrichment (path-derived metadata
  only), no multiline parity beyond pattern/negate/match.
- **No second durability model**: no `buffered`/lightweight memory-spool mode
  (D3); no changes to the eight invariants.
- **No additional sinks** in this program: no Elasticsearch, Loki, S3.
- **No `sinks.workers`** re-introduction without benchmark evidence.
- **No new run mode**: collection is a documented pipeline template plus
  deployment manifests, not a `run.mode: collect`.
- **No dedup / exactly-once**: at-least-once stays the contract.
- **No Planner-style write semantics** (upsert/SCD2): out of scope for log
  collection (already rejected in `competitor-research.md` §9.3).
