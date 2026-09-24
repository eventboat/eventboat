# Collecting logs with Eventboat (files → VictoriaLogs)

Eventboat's log-collection shape is deliberately small: tail **files** (host
or container logs) with the `file` source, normalize with a `script`
transform, and ship **batches** to a VictoriaLogs instance through the
dedicated `victorialogs` sink. There is no second agent — this is the
one-selection deployment the design of record describes
([Log collection mode](design/2026-09-24-log-collection.md)).

## Pipeline shape

[`examples/collector/pipeline.yaml`](../examples/collector/pipeline.yaml) is
the reference; the core:

```yaml
sources:
  logs:
    decoder: json          # one JSON object per line; malformed lines dead-letter
    file: {path: logs/app.jsonl}
transforms:
  stamp:
    depends_on: [logs]
    script: |
      payload.host = constants.host      # per-node identity
      payload.app = constants.app
      if "ts" not in payload:            # backfill from engine metadata
          payload.ts = meta.ingest_time
sinks:
  victorialogs:
    depends_on: [stamp]
    encoder: json                        # the body must be codec-encoded JSON
    batch: {size: 500, timeout_ms: 2000} # one POST per batch
    victorialogs:
      url: http://127.0.0.1:9428         # /insert/jsonline is appended
      stream_fields: host,app
      time_field: ts
```

The `file` source tails one file and persists its byte offset through
`Source.Commit`, so a restart resumes after the committed lines
(at-least-once: duplicates after a crash, never loss). Glob, rotation and
truncation detection, multiline aggregation and container-path metadata are
the file-source v2 work scheduled in the design document
([§2.3–§2.4](design/2026-09-24-log-collection.md)). The example ships a
contract suite (`tests/collector.yaml`: the stamp, the timestamp backfill, the
malformed-line DLQ path) that `eventboat test` runs against the real engine,
and `internal/inttests/victorialogs` drives the same shape into a live
VictoriaLogs and queries the rows back through LogsQL (gated by
`EVENTBOAT_VICTORIALOGS_URL`).

## The `victorialogs` sink

One `Sink.Write` call is exactly one `POST <url>/insert/jsonline`: the body is
the batch's encoded JSON, one object per line, `Content-Type:
application/stream+json`. Query parameters only carry the knobs you set —
omitting one lets VictoriaLogs apply its own default.

| Key | Default | Meaning |
|---|---|---|
| `url` | required | base URL; must be absolute (contain `://`) |
| `stream_fields` | — | `_stream_fields`: fields that identify the stream (recommended `host,app`, plus `namespace,pod` in Kubernetes) |
| `time_field` | — | `_time_field`: the JSON field carrying the event timestamp |
| `msg_field` | — | `_msg_field`: the JSON field carrying the log message |
| `extra_fields` | — | `_extra_fields`: comma-separated fields stored as extra fields |
| `gzip` | `false` | gzip the request body (`Content-Encoding: gzip`) |
| `max_idle_conns` | `2` | `http.Transport.MaxIdleConnsPerHost` (one sink goroutine by design) |
| `timeout_ms` | `10000` | request timeout |
| `account_id` / `project_id` | `0` | tenant headers; `0` omits them |

Error mapping follows VictoriaLogs' own semantics:

- **2xx — committed.** VictoriaLogs *skips unparseable lines server-side and
  still answers 200*, which is why the body must be codec-encoded JSON
  (`encoder: json`) and why the file-source v2 work keeps `max_line_bytes`
  aligned with VL's `-insert.maxLineSizeBytes`: a raw-text body would lose
  lines silently.
- **4xx — permanent.** VL returns 4xx only when the whole batch is
  unparseable; the edge's delivery policy exhausts and the batch dead-letters.
  Re-sending cannot help.
- **5xx / network error / timeout — transient.** Retried per edge policy,
  then dead-lettered. VL pushes back instead of dropping while overloaded.

## Deployment shapes

- **Kubernetes DaemonSet** ([`examples/collector/k8s-daemonset.yaml`](../examples/collector/k8s-daemonset.yaml)):
  one collector pod per node. Mount the host log tree read-only
  (`hostPath /var/log`) and the state directory **read-write**
  (`hostPath /var/lib/eventboat`) — never an `emptyDir`: the file offsets live
  in the SQLite spool and must survive pod restarts. Pipelines and the Runtime
  config come from ConfigMaps; `/live` and `/ready` are the probes; each pod
  is its own single-active writer (the general Kubernetes contract is in
  [Deploying Eventboat on Kubernetes](k8s.md)).
- **VM / bare metal** ([`systemd/`](../examples/collector/systemd)):
  the unit template plus logrotate guidance — prefer `create` mode;
  `copytruncate` rewrites the file in place and the tailer's offset no longer
  matches (duplicates, and a silent stall until the P2 hardening lands).
- **Mixed estates**: per-role pipeline files under `--config-dir`, with
  per-host values injected through `${VAR}` substitution (the DaemonSet
  template feeds the node name into the `host` constant this way).

## Backpressure

VictoriaLogs **pushes back rather than dropping** under load, and that is what
the engine's admission gate turns into end-to-end backpressure:

```
VL overloaded (5xx / timeout)
  → Sink.Write blocks and is retried per the edge's delivery policy
  → the batch stays uncommitted; the spool keeps the rows
  → uncommitted rows reach limits.max_in_flight (10 000) and the admission
    gate blocks the source's emit
  → the file source stops reading; the unread data stays in the file
```

Nothing is dropped in that chain, and the file itself is the buffer. Size
`limits.max_in_flight ≈ peak rows/s × tolerated outage seconds` — at 5 000
rows/s the default absorbs about two seconds, which is why it is the first
parameter to raise in production. The file byte offset only advances for
committed lines, so a collector restart after an outage re-reads nothing that
was already shipped.

## Tuning

`batch.size` is the throughput lever (one POST per batch; a sink has exactly
one goroutine by design). The symptom → metric → knob tables, sizing formulas
and the guardrails for the new knobs live in the design document
([§2.6](design/2026-09-24-log-collection.md)); start there before changing
anything.
