# Tuning log collection (files → VictoriaLogs)

This is the operator's companion to [Collecting logs with Eventboat](collector.md):
what each exposed knob does, which metric tells you when to change it, how to
size the two buffers that actually matter (`max_in_flight` and the spool), and
how to reproduce the numbers on your own hardware. The design of record is
[Log collection mode](design/2026-09-24-log-collection.md) §2.6; this page
tracks the implemented surface — when the two disagree, the code
(`internal/runtimecfg`, the plugin schemas) wins.

## Two layers of knobs

The config system already splits deployment from pipeline, and the knobs
follow it:

- **Runtime** (`kind: Runtime`, usually `eventboat.yaml`): storage location,
  group-commit write batch, telemetry, admin. One per process; every pipeline
  in that process shares it.
- **Pipeline** (`kind: Pipeline`): per-pipeline semantics — sources,
  transforms, sinks, edges, `limits`, `telemetry` (sampling and redaction).
  Each deployed pipeline can differ.

**Defaults first.** The common case is fast with zero configuration: the
store's group commit is default-on (`storage.write_batch` 256 rows / 2 ms),
the file source's line bound is aligned with VictoriaLogs (256 KiB), and a
sink that forgets `batch` still works (one message per `Write`). Knobs exist
only where a genuine tradeoff is user-visible; each one in the tables below
has a metric and a direction. Everything is schema-validated, LSP-completed
and shown by `eventboat verify`/`eventboat explain`; the strict loader rejects
unknown keys, so a knob that is not in these tables does not exist.

## Runtime knobs (`kind: Runtime`)

| Key | Default | Effect | Tune when |
|---|---|---|---|
| `storage.data_dir` | `data` | SQLite spool, source states, dead letters, lease sidecar | the state must survive restarts (DaemonSet `hostPath` — never an `emptyDir`) |
| `storage.ephemeral` | `false` | in-memory store; nothing survives a restart | tests and demos only — never a production collection run |
| `storage.spool_retention` | `10000` | rows kept **behind the durable checkpoint** before the trim deletes them | lower to bound disk; raise for `replay --spool` disaster drills or very slow restarts |
| `storage.write_batch.max_rows` | `256` | ceiling on one group-commit multi-row `INSERT` | raise when many concurrent sources can fill a group; a single hot file cannot |
| `storage.write_batch.max_wait_ms` | `2` | ceiling on a queued append's wait for companions, realized as bounded scheduler yields (not a sleeping timer); `0` = write-through | lower for latency; raise only with evidence that batches are not forming |
| `telemetry.prometheus` | `true` | serves `/metrics` on the admin listener | leave on in production |
| `telemetry.otlp_endpoint` | — | OTLP/HTTP push (10 s) for metrics and traces | when a central collector scrapes/pushes instead |
| `telemetry.sample_ratio` | `0.1` | trace sampling for the OTLP exporter | set `0` under load (per-message spans are separately opt-in) |
| `admin.listen` / `admin.enable` | `127.0.0.1:7788` / `true` | ops surface and `/metrics` | remote scraping wants a non-loopback bind **and** `admin.token` |

There is **no `storage.checkpoint_interval_ms`**. The design trimmed it in P1
(§2.6.2) and it never shipped: the engine's `persistMu` serializes checkpoint
advances and the store writer coalesces them, but the durability barrier
(`durableThrough >= committedThrough`) depends on an advance being flushed —
making `SetCheckpoint` fire-and-forget by interval would weaken it. See
[Persistence](developer/02-engine.md#persistence) for the current contract.

## Pipeline knobs

### Sources, transforms, edges

| Key | Default | Effect | Tune when |
|---|---|---|---|
| `limits.max_in_flight` | `10000` | uncommitted-message admission cap = backpressure depth = **outage buffer** | size it: `peak rows/s × tolerated sink-outage seconds` (see [Sizing](#sizing-rules)) |
| `limits.drain_timeout` | `10s` | graceful shutdown wait before the hard cancel | large batches or slow sinks |
| sink `batch.size` | `1` | messages per `Sink.Write` — for VictoriaLogs, one POST per batch | collection should ship 500–1000 (the `lint_collector_batch_one` lint catches a forgotten one) |
| sink `batch.timeout_ms` | `1000` | flush a partial batch after this long | raise (1–5 s) when arrival is slow and requests are small; lower to bound commit latency |
| transform `workers` | `1` | per-transform-node concurrency | CPU-bound script/WASM nodes (`eventboat_script_duration_seconds` p95 high) |
| edge `buffer.max_events` | `128` | per-edge in-memory surge buffer | burst smoothing only — it is not a reliability mechanism |
| edge `delivery.retries` | `3` | retry attempts per batch | remote sinks that push back (VictoriaLogs does, rather than dropping) |
| edge `delivery.backoff` | `exponential` (100 ms base) | retry spacing, also `constant` | shorten only for fast recovery; lengthen to avoid retry storms |
| edge `delivery.timeout_ms` | `30000` | per-attempt bound | slower or faster remote sinks |
| edge `required` | `true` | whether a failed edge fails the batch or drops it | `false` isolates an audit/best-effort branch (drops are counted) |
| pipeline `telemetry.span_sample_rate` | `0` | per-message spans | keep `0` at collection rates; `1` for debugging only |
| pipeline `telemetry.redact` | — | presentation-only masking for tail/DLQ views | payloads with secrets; it never alters stored rows |
| pipeline `dlq.retention` | `0` (keep forever) | prune dead letters older than this | only when a DLQ backlog is deliberate; removed rows are gone from `replay` |

### `file` source

| Key | Default | Effect | Tune when |
|---|---|---|---|
| `poll_every_ms` | `250` | glob rescan **and** per-file poll interval | lower for faster discovery/rotation latency; raise when thousands of files make scanning expensive |
| `start_at` | `beginning` | where a newly discovered file starts | `end` to skip data written before the collector started |
| `on_eof` | `tail` | `tail` polls forever; `stop` finishes when every tracked file is read (batch/job shape) | `stop` for complete batch files |
| `close_inactive_ms` | `300000` | close an idle descriptor; it reopens when the file changes | lower to bound open fds on hosts with many files (`0` = never close) |
| `ignore_older_ms` | `0` | skip files older than this **at first discovery** | rollouts that must not ingest historic logs |
| `clean_removed` | `true` | drop the state of files that leave the glob once drained | `false` to keep the state across a temporary disappearance |
| `max_line_bytes` | `262144` (256 KiB) | read-buffer cap and oversize threshold | must stay `<=` VictoriaLogs' `-insert.maxLineSizeBytes` (see [Line bound](#line-bound)) |
| `oversize` | `truncate` | `truncate` emits the first N bytes; `skip` drops the line and counts it | `skip` when a truncated line is useless downstream (e.g. binary payloads) |
| `multiline.max_lines` | `500` | flush an open group at this many lines | memory ↔ group completeness |
| `multiline.max_bytes` | `1048576` (1 MiB) | flush an open group at this many bytes | memory ↔ group completeness |
| `multiline.timeout_ms` | `2000` | flush an open group after this much idle time; **`0` = no time-based flush** | lower for latency; keep `> 0` unless you accept an isolated trailing group waiting for the next group-starting line |
| `host` | `os.Hostname()` | value carried as `meta.host` | set per role/estate when hostnames are not unique |

### `victorialogs` sink

One `Sink.Write` is exactly one `POST <url>/insert/jsonline`; there is no
`sinks.workers` knob (the loader rejects it) — a sink has one goroutine by
design, and `batch.size` is the throughput lever.

| Key | Default | Effect | Tune when |
|---|---|---|---|
| `url` | required | base URL; `/insert/jsonline` is appended | — |
| `stream_fields` | — | `_stream_fields`: the fields that identify the stream | recommended `host,app`, plus `namespace,pod` in Kubernetes |
| `time_field` | unset (VL's `_time`) | `_time_field`: the event-time field | payload carries its own timestamp |
| `msg_field` | — | `_msg_field`: the message field | always for text logs |
| `extra_fields` | — | `_extra_fields`: comma-separated stored-computed fields | — |
| `gzip` | `false` | gzip the request body | WAN links or many large batches (CPU ↔ bytes) |
| `max_idle_conns` | `2` | transport idle-connection pool | leave; one sink goroutine reuses one connection |
| `timeout_ms` | `10000` | request timeout | slow links; lower to fail over faster |
| `account_id` / `project_id` | `0` | tenant headers; `0` omits them | multi-tenant VictoriaLogs |

## Sizing rules

### Outage buffer: `limits.max_in_flight`

```
limits.max_in_flight ≈ peak rows/s × tolerated VictoriaLogs outage seconds
```

The default 10 000 absorbs **2 seconds** at 5 000 rows/s — and only 0.2 s at
50 000 rows/s, which is why it is the first parameter to raise in production.
Cost: each uncommitted row occupies roughly 100–200 bytes in the commit
tracker (`outstanding`, `srcRefs`, `acceptedAt` maps) plus its spool row on
disk; 100 000 rows is tens of MB of tracker and a few hundred MB of spool at
1 KiB rows. During an outage nothing is trimmed (the checkpoint stalls;
retention is anchored on durable progress), so the backlog is bounded by this
knob, not by `storage.spool_retention`. The **file itself is also a buffer**:
when the cap is reached the source stops reading and the host's log files keep
growing. Nothing is dropped.

### Disk: spool and dead letters

```
steady-state spool bytes ≈ storage.spool_retention × row size × 2–3
row size ≈ raw line + meta (file_path, host, container fields) + JSON envelope
```

SQLite's WAL and page overhead make 2–3× a safe multiplier. At 512 B lines
and ~150 B of meta, a 10 000-row retention is ~15–20 MB; a 300 000-row outage
backlog (60 s at 5 000 rows/s) is ~0.5 GB. `storage.spool_retention` is the
**crash-replay window** (rows kept behind the checkpoint for
`replay --spool`), not an outage buffer: retention never trims ahead of the
durable checkpoint, so a stalled checkpoint means a growing spool regardless
of the retention value. Dead letters are separate and kept forever unless the
pipeline sets `dlq.retention`.

### Group commit: `storage.write_batch.*`

The store has one writer goroutine; a batch is formed from the appends queued
at the same moment, and `max_rows` (256) is a **ceiling**, not a trigger. A
single hot file emits one line at a time (`one_source` shape), so its group is
a group of one — raising `max_rows` cannot help; the serial accept path is the
limit (~8 K lines/s on the reference machine). Many concurrent files are where
grouping fills: group commit brought parallel appends from ~71 µs to
~11–13 µs each on the reference machine. `max_wait_ms` is a probe budget of
scheduler yields, not a sleep: `0` writes through, larger values only help if
companions exist.

### Line bound

Keep `max_line_bytes <=` VictoriaLogs' `-insert.maxLineSizeBytes` (both default
to 256 KiB). A line above VL's limit is **skipped server-side while VL still
answers 200**: no eventboat counter moves, and the only visible trace is
VictoriaLogs' `vl_http_errors_total`. With `oversize: truncate` the truncated
line is still a valid JSON object (the envelope is ours), so VL stores a
shorter event; with `oversize: skip` eventboat counts it in
`eventboat_source_lines_skipped_total` and never ships it. If you lower VL's
limit, lower ours too.

### Multiline timeout

`multiline.timeout_ms: 0` disables the time-based flush. A trailing group is
then only flushed by the next group-starting line, `max_lines`/`max_bytes`,
rotation/truncation or stop-mode EOF. On a quiet file the group can sit
uncommitted indefinitely — and because a group is deliberately not flushed at
shutdown, a restart re-reads it (duplicates, never loss). The cost is latency,
not data; `lint_multiline_no_timeout` warns when `0` is set explicitly. Keep
`close_inactive_ms` well above the inter-line gap of your slowest stack trace:
closing an idle descriptor flushes the open group first.

### Restarts and duplicates

There is no checkpoint-interval knob (see above). The duplicate window after
a hard crash is the set of rows that were accepted but not yet durable and
delivered — bounded by `limits.max_in_flight` plus the rows already beyond the
durable checkpoint. Lowering `max_in_flight` and `multiline.timeout_ms`
shrinks that window; a faster commit path (sink latency) shrinks it too.
`copytruncate` rotation re-reads the file from 0 by design — prefer logrotate
`create` mode.

## Symptom → metric → knob

Metric names are the Prometheus exposition at `/metrics`; `eventboat_source_*`
counters are per `{pipeline,node}` and exported as deltas of the source
plugin's own counters (a source restart re-baselines them).

| Symptom | Look at | Likely cause | Knobs |
|---|---|---|---|
| **Collection throughput below the host's write rate; the file drains too slowly** | `eventboat_in_flight_messages` pinned at the cap, `eventboat_backpressure_events_total` climbing, `eventboat_source_lines_read_total` rate below the host's | the downstream chain (store, transform, sink) is saturated | first fix the saturated stage below; as a buffer decision raise `limits.max_in_flight` |
| **… and the store looks slow** | `eventboat_spool_append_seconds` p99 much larger than `storage.write_batch.max_wait_ms`, low append rate | one hot file serializes appends (group of one); disk is slow | accept the serial limit, add files/sources, or raise `storage.write_batch.max_rows` only when many sources run |
| **… and transforms look slow** | `eventboat_script_duration_seconds` p95, `eventboat_wasm_transform_duration_seconds` | CPU-bound scripts on one worker | raise transform `workers`; move work to `fields`; check the step budget (`eventboat_script_step_budget_exhausted_total`) |
| **VictoriaLogs slow or overloaded** | `eventboat_sink_write_duration_seconds` p95/p99, `eventboat_delivery_retries_total`, `vl_http_errors_total` (5xx/429) | too many small requests, WAN bytes, VL under-provisioned | raise sink `batch.size` (500 → 1000), raise `batch.timeout_ms` if batches arrive partial; `victorialogs.gzip: true`; raise `delivery.retries`/`timeout_ms`; then scale VL |
| **Disk keeps growing** | `eventboat_spool_depth` (rows beyond checkpoint), data-dir size, `eventboat_dead_letter_total` | sink is behind (checkpoint stalls, retention cannot trim) or dead letters accumulate | fix the sink; steady state: lower `storage.spool_retention`; dead letters: set `dlq.retention` (opt-in); outage backlog: lower `limits.max_in_flight` |
| **Memory high / GC pressure** | `eventboat_in_flight_messages`, `eventboat_spool_depth` (only under `--ephemeral`), process RSS (the `/metrics` registry is Eventboat's own — no `go_*` runtime collectors) | the outage buffer, multiline groups or edge buffers are large | lower `limits.max_in_flight`; lower `multiline.max_lines`/`max_bytes`; lower edge `buffer.max_events`; `telemetry.span_sample_rate: 0` + Runtime `telemetry.sample_ratio: 0`; `GOGC`/`GOMEMLIMIT` live in the deployment manifest, not in Eventboat config |
| **Commit latency too high** | `eventboat_commit_latency_seconds` p95/p99, `eventboat_spool_append_seconds` | batching waits (store group commit, sink batch, edge retries) | lower sink `batch.size`/`batch.timeout_ms`; lower `storage.write_batch.max_wait_ms` (even `0`); lower `delivery.timeout_ms`; lower `multiline.timeout_ms` — each buys latency with throughput |
| **Lines skipped or truncated** | `eventboat_source_lines_skipped_total`, `eventboat_source_lines_truncated_total`, `eventboat_decode_errors_total`, `vl_http_errors_total` | a producer writes lines above `max_line_bytes`, or ours exceeds VL's limit (server-side silent skip) | align `max_line_bytes` with VL's `-insert.maxLineSizeBytes`; choose `oversize: skip` vs `truncate`; fix the producer if the size is unexpected |
| **Lines stop/stall after rotation** | `eventboat_source_rotations_total` rising while `eventboat_source_lines_read_total` is flat; host fd count | too-slow discovery, fd pressure, or the rotated file was deleted before it was drained | lower `poll_every_ms`; lower `close_inactive_ms` (or set `0` on modest file counts); check logrotate retention (`maxsize`, delete policy) keeps rotated files long enough |
| **Duplicates grow after a restart** | `eventboat_spool_depth` before the crash, duplicate rows in VL queries | uncommitted rows were re-read (at-least-once), or `copytruncate` re-read from 0 | lower `limits.max_in_flight` and `multiline.timeout_ms` to shrink the window; prefer logrotate `create`; duplicates are the contract, not loss |
| **Dead letters grow** | `eventboat_dead_letter_total{pipeline,node,reason_class}` by class, `eventboat_dlq_write_failures_total` | `decode`: malformed lines; `delivery`: sink rejects after retries; `encode`: payload not encodable | fix the producer or the sink; size `dlq.retention` against the backlog; a nonzero `dlq_write_failures` blocks commits — act immediately |
| **Pipeline unexpectedly paused** | `eventboat_pipeline_paused` | admin pause/drain, or a failed run left the pipeline stopped | check `/admin/status.json` and the process log; resume via the admin surface |

## Monitoring and alerting

Eventboat exposes `/metrics` on the admin listener when
`telemetry.prometheus` is true (default). The gauges
(`eventboat_in_flight_messages`, `eventboat_spool_depth`,
`eventboat_pipeline_paused`) are pushed on ops status snapshots, so a
scrape picks up the last recorded value — poll `GET /admin/status.json`
(the admin UI/SSE does it continuously) or the MCP `status` tool to keep
them fresh. A single `run --config` process has no HTTP surface and exports
via OTLP only. VL-side metrics live on VictoriaLogs' own `/metrics`.

**VictoriaLogs' `vl_http_errors_total` is the only face of lines VictoriaLogs
rejects or skips while still answering 200.** Eventboat cannot see those.
Alert on it, and keep `max_line_bytes <= -insert.maxLineSizeBytes`.

Minimal alert set (thresholds are starting points; tune per estate):

| Signal | Condition | Why it matters | First action |
|---|---|---|---|
| Backpressure sustained | `rate(eventboat_backpressure_events_total[5m]) > 0` for 10 min with `eventboat_in_flight_messages` near `limits.max_in_flight` | the sink is slower than the host's log rate | check `eventboat_sink_write_duration_seconds`, VL health, then the symptom table |
| DLQ growth | any `increase(eventboat_dead_letter_total[5m])`, and `eventboat_dlq_write_failures_total > 0` | bad lines or a rejecting sink; write failures block commits | inspect `dlq_query` with `reason_class`; fix producer/sink |
| Source stall | `increase(eventboat_source_lines_read_total[5m]) == 0` while logs are expected | tail stopped discovering/reading (rotation, fd, filesystem) | check `eventboat_source_rotations_total`, `poll_every_ms`, host fd limits |
| VL errors | `rate(vl_http_errors_total[5m]) > 0` | lines skipped/refused server-side with 200 responses | compare `max_line_bytes` with `-insert.maxLineSizeBytes`; check VL disk/capacity |
| Spool failures | `increase(eventboat_spool_failures_total[5m]) > 0` | a message could not be made durable (not delivered) | check the data dir, disk full, store errors in the log |
| Spool replay window | `eventboat_spool_depth` growing without a sink outage | checkpoint not advancing | check `eventboat_sink_write_duration_seconds` and the store log line |
| Data-dir free space | free space on `storage.data_dir` below 20% | the spool grew — the pipeline will refuse writes when it is full | find why the checkpoint stopped; add disk (lowering `storage.spool_retention` does not trim while the checkpoint stalls) |

## Measuring on your own hardware

`scripts/bench-collect.sh` runs the end-to-end shape — real file source →
real engine and group-commit SQLite spool → the real `victorialogs` sink
against an in-process HTTP server — with a configurable line size, line count
and arrival rate (`EVENTBOAT_BENCH_LINES`, `EVENTBOAT_BENCH_RATE`). The
reference table from the reference dev machine (i5-14600KF, Windows, Go
1.25.0, 2026-09-25; 20 000 lines, sink batch 500 / 100 ms, store defaults):

| Shape | Wall time | Throughput | Bytes/s |
|---|---|---|---|
| 512 B lines | 2.3–2.7 s | 7 400–8 700 lines/s | 3.8–4.5 MB/s |
| 4 KiB lines | 3.4–4.3 s | 4 650–6 100 lines/s | 19–25 MB/s |
| 512 B @ 5 000 lines/s target | ~4.0 s | ~5 000 lines/s (kept up) | ~2.6 MB/s |

Read these as order-of-magnitude, machine-local numbers. The benchmark shape
is deliberately the **single hot file**: the accept path is serial there, so
this is the per-file floor, not the engine's aggregate ceiling (the
`bench-gate.sh` shapes with 4/16 concurrent sources reach higher). Two
disciplines matter:

1. **Each shape runs in its own process.** These numbers are sensitive to what
   ran earlier in the same `go test` process (calibration history, cache
   warmth — the S2 finding recorded in `bench-gate.sh`); the script already
   isolates them, so do not merge shapes into one `-bench` regex.
2. **`scripts/bench-gate.sh` is a loose regression gate, not a promise.** Its
   limits are 5–25× the reference values to absorb shared-runner noise; a
   green gate says "no order-of-magnitude regression". Sizing decisions come
   from `bench-collect.sh` on the target hardware.

Rate mode answers the operational question directly: set
`EVENTBOAT_BENCH_RATE` to the host's peak lines/s and compare the reported
`lines/s` with the target. If the reported rate drops below the target, the
collector cannot keep up at that rate and `eventboat_backpressure_events_total`
would be climbing in production — raise the saturated stage's knob instead of
the rate.

## Guardrails

Three verify lints (warnings; `--strict` escalates them) catch the dangerous
collection configurations before they run:

- `lint_line_bytes_over_vl` — `max_line_bytes > 262144` in a pipeline
  with a `victorialogs` sink: VL will skip longer lines silently.
- `lint_multiline_no_timeout` — an explicit `multiline.timeout_ms: 0`: an
  isolated trailing group waits for the next group-starting line.
- `lint_collector_batch_one` — a `file` source with a real sink whose
  effective `batch.size` is `1`: the collection shape wants batching (one
  POST/write per batch).

The Runtime loader validates the group-commit surface at load time: unknown
keys and type errors are rejected, `storage.write_batch.max_rows >= 1`,
`storage.write_batch.max_wait_ms >= 0`, `storage.spool_retention >= 0` and
`telemetry.sample_ratio ∈ [0, 1]`. There is no separate effective-values echo
at startup today; `eventboat verify` plus this page are the effective-value
reference.

## Deliberately not exposed

- **SQLite pragmas.** WAL + `synchronous=NORMAL` is the documented durability
  contract, not a tradeoff to rediscover.
- **GC.** `GOGC`/`GOMEMLIMIT` belong in the deployment manifest (systemd
  `Environment=`, container env); Eventboat does not re-express them.
- **Internal pool sizes** and channel internals beyond `buffer.max_events`.
- **Sink worker counts.** One sink goroutine is the design; `sinks.workers`
  is rejected by the loader. Re-introducing concurrency needs benchmark
  evidence that one batch-POST worker saturates.
- **`script.max_steps`.** The Starlark step budget is hard-coded at 100 000
  (`starhost.DefaultOptions`) and is not a config key today.
