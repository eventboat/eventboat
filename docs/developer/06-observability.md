---
title: "Observability & operations"
order: 6
---

# Observability & operations

Eventboat exposes three operations surfaces that share one implementation:
the MCP tools, the Admin REST endpoints (plus SSE and an embedded read-only
UI), and the `--json` CLI outputs are all thin shells over
`internal/ops.Service` — REST, MCP and CLI emit the same Go structs and JSON
shapes. Telemetry is OpenTelemetry with dual export (Prometheus pull +
OTLP/HTTP push); the metric set is fixed and enumerated in
`internal/obs/obs.go`.

## Metrics

One MeterProvider, two readers (`internal/obs/obs.go`): Prometheus
exposition at `/metrics` and, when `telemetry.otlp_endpoint` is set, an
OTLP/HTTP push reader (10s interval). Fully disabled telemetry costs nothing
(noop providers; every `Obs` helper is nil-receiver safe). All 28
instruments carry the `eventboat_` prefix:

### Counters

| Metric | Labels | Counts |
|---|---|---|
| `eventboat_messages_in_total` | pipeline, source | messages accepted into the spool |
| `eventboat_messages_committed_total` | pipeline | messages reaching a terminal state |
| `eventboat_dead_letter_total` | pipeline, node, reason_class | dead-lettered messages |
| `eventboat_dlq_write_failures_total` | pipeline | failed dead-letter writes (commit blocked) |
| `eventboat_cel_eval_errors_total` | pipeline, edge, lang | predicate evaluation errors (treated as not-passed); `lang` is `cel` or `cesql`, the name keeps "cel" for M2 continuity |
| `eventboat_fanout_no_match_total` | pipeline, node | messages filtered by zero matching edges (incl. zero transform outputs) |
| `eventboat_delivery_retries_total` | pipeline, node | delivery retry attempts |
| `eventboat_optional_drops_total` | pipeline, edge | drops on failed `required: false` edges |
| `eventboat_decode_errors_total` | pipeline, source | decode failures at entry |
| `eventboat_spool_failures_total` | pipeline | spool append failures (message not delivered) |
| `eventboat_backpressure_events_total` | pipeline, source | admissions blocked by the high watermark |
| `eventboat_script_step_budget_exhausted_total` | pipeline, node | Starlark executions hitting the 100k step budget |
| `eventboat_wasm_timeouts_total` | pipeline, node | WASM invocations killed by `timeout_ms` |
| `eventboat_jobs_started_total` | pipeline, trigger | job runs started |
| `eventboat_jobs_overlap_skipped_total` | pipeline | triggers rejected by `overlap: skip` |
| `eventboat_jobs_catchup_skipped_total` | pipeline | missed ticks outside `catchup_window` |
| `eventboat_jobs_completed_total` | pipeline, status | runs completed by terminal status |
| `eventboat_job_rows_read_total` | pipeline | rows read by job runs |
| `eventboat_job_rows_delivered_total` | pipeline | rows delivered by job runs |
| `eventboat_plugin_restarts_total` | plugin | supervisor respawns of crashed gRPC plugin processes (`grpc.restart: restart`) |

`reason_class` is a coarse prefix class of the dead-letter reason — one of
`script`, `decode`, `codec`, `delivery`, `encoder`, `canceled`, else
`other` (`obs.ReasonClass`).

### Histograms (unit: seconds)

| Metric | Labels | Observes |
|---|---|---|
| `eventboat_script_duration_seconds` | pipeline, node | Starlark script execution |
| `eventboat_wasm_transform_duration_seconds` | pipeline, node | WASM invocations (killed calls never reach it) |
| `eventboat_sink_write_duration_seconds` | pipeline, node | sink batch writes (each attempt) |
| `eventboat_job_duration_seconds` | pipeline, status | job run wall-clock |
| `eventboat_commit_latency_seconds` | pipeline | accept-to-commit latency |

### Gauges

| Metric | Labels | Reports |
|---|---|---|
| `eventboat_in_flight_messages` | pipeline | uncommitted messages in execution |
| `eventboat_spool_depth` | pipeline | spooled messages beyond the checkpoint (`arrivedMax - committedThrough`) |
| `eventboat_pipeline_paused` | pipeline | 1 when paused, else 0 |

Gauges are pushed on every `ops.Status()` snapshot (the SSE stream and UI
poll status; `/metrics` scrapes the recorded values).

## Tracing

- **One span per job run**: `eventboat.job.run`, attributes
  `pipeline`/`run_id`/`trigger`, ended with the terminal status and recorded
  errors (`internal/jobs`). Script backtraces ride dead-letter records and
  span error events.
- **Per-message spans are opt-in**: `telemetry.span_sample_rate` (default 0
  = none, zero cost; 1 = all). Roots are named `eventboat.message` with
  `pipeline`/`message_id`/`source`/`spool_seq` attributes and end at a
  terminal state — `eventboat.terminal_state` = `committed` or
  `dead_letter` (`internal/engine/engine.go`, `startMessageSpan`/`finishSpan`).
  Rationale (review R16): no per-message spans at routing rates. The span
  context is deliberately **not** propagated through the DAG — correlation
  rides the attributes.
- Trace export goes through OTLP/HTTP when `telemetry.otlp_endpoint` is
  configured, sampled by `telemetry.sample_ratio` (default 0.1); otherwise
  the global noop tracer.

## OTel wiring

`obs.Setup(ctx, cfg)` builds the providers from the Runtime config
(`internal/runtimecfg`): `telemetry.prometheus` (default true) registers the
Prometheus reader served by the admin listener at `/metrics`;
`telemetry.otlp_endpoint` adds the OTLP metric reader (push every 10s) and
the trace exporter. Single-pipeline `eventboat run` gets OTLP only (no HTTP
surface); `run --config-dir` and `mcp --http` serve both readers.

## The admin surface

`internal/admin` serves the whole surface on one mux — Admin REST + SSE +
UI, `/metrics`, `/mcp` — behind one security middleware. Auth: an optional
bearer token resolved as `--admin-token` flag > `EVENTBOAT_ADMIN_TOKEN` env
> `admin.token` in the Runtime config (`internal/cli/mcp.go`,
`internal/runtimecfg`). With a token set, every request needs
`Authorization: Bearer <token>` (the `?token=` query form exists only for
the UI's EventSource and first navigation — it then appears in browser
history/proxy logs; use the header from curl/agents). A non-loopback
`admin.listen` **refuses to start without a token** — the write surface can
deploy pipeline YAML whose grpc plugin `command:` executes on the host
(`internal/admin/security.go`). Loopback binds additionally enforce a Host
allowlist (DNS-rebinding defense); wildcard binds skip it and rely on the
mandatory token. JSON request bodies are capped at 8 MiB
(`maxBodyBytes`, `internal/admin/admin.go`).

| Method | Path | Purpose |
|---|---|---|
| GET | `/admin/status.json` | status snapshot of every deployed pipeline (mode, per-node list, in-flight, checkpoint, counters, msg/s, recent runs) |
| GET | `/admin/jobs/{pipeline}?limit=` | job run history |
| GET | `/admin/tail/{node}?n=` | recent sampled deliveries for one node |
| GET | `/admin/dlq/{pipeline}?since=&where=` | dead-letter query (`since` duration, `where` CEL predicate over `{payload, meta}`); response is **redacted** |
| GET | `/admin/catalog.json` | the plugin catalog with versions and schemas |
| POST | `/admin/deploy` | body `{"config": <yaml>}`; verify-first, deploy rejected on any verify error |
| POST | `/admin/trigger/{pipeline}` | body `{"parameters": {...}, "wait": bool}`; fires a job run |
| POST | `/admin/replay` | body `{"pipeline", "ids", "at"}`; re-injects dead letters |
| POST | `/admin/drain/{pipeline}` | stop sources, wait for in-flight commit; stays deployed |
| POST | `/admin/pause/{pipeline}` | stop the instance (resume from persisted states) |
| POST | `/admin/resume/{pipeline}` | restart a paused pipeline |
| GET | `/admin/sse` | Server-Sent Events: `deploy`/`job`/`status` events plus a full status snapshot every second |
| GET | `/metrics` | Prometheus exposition |
| GET/POST/DELETE | `/mcp` | MCP Streamable HTTP (when enabled) |
| GET | `/admin/` | the embedded read-only UI (`/admin` redirects) |

The **admin UI** is a single embedded HTML page (`internal/admin/ui.go`):
read-only status console fed by the SSE stream; on a 401 it serves a sign-in
prompt that verifies the token against `/admin/status.json` and keeps it in
`sessionStorage`.

The same operations exist as **14 MCP tools** (`internal/mcpserver`):
`catalog`, `verify`, `test`, `explain`, `status`, `jobs`, `tail`,
`dlq_query`, `deploy`, `trigger`, `dlq_replay`, `drain`, `pause`, `resume` —
over stdio (`eventboat mcp --stdio`) or HTTP (`--http`).

## Log tailing

`ops.Tail` samples the most recent deliveries **per sink node** (bounded
ring of 100 entries, payloads truncated at 512 bytes, `is_replay` flagged)
via a sink wrapper installed on every managed engine. Entries show the
payload document as delivered (`msg.Out`, falling back to `Raw`). Consume it
via `GET /admin/tail/{node}`, the MCP `tail` tool, or the UI.

## Redaction

The pipeline-level `telemetry.redact` section lists dot-separated field
paths where every segment is a `path.Match` glob (`payload.user.email`,
`meta.authorization`). Compilation and matching live in
`internal/ops/redact.go`. Coverage is deliberately presentation-only:

- **Tail entries** are masked (`"***"`) before the 512-byte truncation;
- **DLQ display** masks both roots — payload patterns against the raw
  document, `meta.*` patterns against the meta map — before letters cross
  the admin REST or MCP `dlq_query` surface (`ops.redactDeadLetters`).

The **stored rows stay raw**: the spool, dead letters and deliveries are the
data path and are never altered, so `DeadLetterReplay` re-injects the
original bytes. Non-JSON tails pass through unmasked. A redact pattern that
cannot compile is a verify error (`telemetry_redact_pattern`) — a bad
pattern must not silently never match.

## Engine counters (the non-OTel layer)

Beside the OTel instruments, the engine keeps atomic counters in
`engine.Metrics` (`internal/engine/engine.go`): `MessagesIn`,
`CommittedCount` (split from the checkpoint pointer, review R5),
`CheckpointPtr`, `DeadLettered`, `CelEvalErrors`, `NoMatch`, `Retries`,
`DlqFailures`, `OptionalDrops`, `DecodeErrors`, `TransformRuns`,
`Backpressured`, `SpoolFailures`. These are the M1 bookkeeping layer and the
source of the `--json` CLI/status output — the OTel instruments record the
same events for Prometheus/OTLP consumers. `ops.Status()` reads them per
pipeline (`messages_in`, `committed`, `dead_lettered`, `checkpoint`,
`in_flight`, `messages_per_sec` as a delta over the last snapshot window).

## SSE event types

`GET /admin/sse` emits `hello` on connect, then typed events — `deploy`
(pipeline deployed, replaced instance), `job` (job run state), `status`
(pause/resume/drain and a full status snapshot every second, which is also
what pushes the gauges). Slow consumers drop events; snapshots repeat.

## HTTP server posture

The admin listener (`admin.Serve`) sets `ReadHeaderTimeout` 10s,
`ReadTimeout` 60s, `IdleTimeout` 120s — and a deliberately loose
`WriteTimeout` (15m) because SSE responses are long-lived streams; it caps
their lifetime instead of erroring mid-request (the UI's EventSource
reconnects).

## Operations runbook pointers

- Status polling: `GET /admin/status.json` or watch `/admin/sse`; job runs
  via `GET /admin/jobs/{pipeline}` (the status embeds the 5 most recent runs,
  surfacing `run:<status>` for job pipelines).
- A wedged WASM worker (fast mode) shows as one transform node stalled with
  a slow-call watchdog log line; restart clears it.
- Backpressure is visible as `eventboat_backpressure_events_total` climbing
  with `eventboat_in_flight_messages` pinned at the high watermark.
- Checkpoint lag is `eventboat_spool_depth`; a growing lag with a failing
  store indicates checkpoint writes failing (replay window widening — the
  engine keeps committing in memory and retries durability).
