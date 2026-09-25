# Collecting logs with Eventboat (files → VictoriaLogs)

Eventboat's log-collection shape is deliberately small: tail **files** (host
or container logs) with the `file` source, normalize with a `fields` transform
(static values plus `meta.*` passthrough) and a `script` transform for
computed values, and ship **batches** to a VictoriaLogs instance through the
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
    file:
      path: logs/*.jsonl   # a glob: rotation and files appearing later are picked up
transforms:
  stamp:
    depends_on: [logs]
    fields:
      fields:
        host: "${constants.host}"   # per-node identity (${VAR}/${constants.x} substituted at load)
        app: "${constants.app}"
  backfill:
    depends_on: [stamp]
    script: |
      if "ts" not in payload:            # backfill from engine metadata
          payload.ts = meta.ingest_time
sinks:
  victorialogs:
    depends_on: [backfill]
    encoder: json                        # the body must be codec-encoded JSON
    batch: {size: 500, timeout_ms: 2000} # one POST per batch
    victorialogs:
      url: http://127.0.0.1:9428         # /insert/jsonline is appended
      stream_fields: host,app
      time_field: ts
```

The `file` source tails a **glob** of files and persists a per-file byte
offset through `Source.Commit`, so a restart resumes every file after its
committed lines (at-least-once: duplicates after a crash, never loss).
Rotation (rename + recreate), copytruncate (in-place truncation), deleted
files and retargeted links are detected through the platform file identity
(device + inode on Unix, volume serial + file index on Windows): the matched
link path is used for glob matching and `meta.file_path`, the identity of the
opened target decides rotation. Multiline aggregation and container-path
metadata (`namespace`/`pod`/`container`) are built in (design §2.4 / §2.3).

| Key | Default | Meaning |
|---|---|---|
| `path` | required | glob (`*`, `?`, `[...]`) of files to tail; a path without metacharacters is one file |
| `poll_every_ms` | `250` | scan interval: glob rescan plus per-file poll |
| `start_at` | `beginning` | where a newly discovered file starts (`end` skips what is already written) |
| `on_eof` | `tail` | `tail` keeps polling for appended lines; `stop` finishes the source once every tracked file is read to its end — the batch/job shape |
| `close_inactive_ms` | `300000` | close a file's descriptor after this much idle time; it reopens when the file changes (`0` = never close) |
| `ignore_older_ms` | `0` | skip files whose mtime is older than this when first discovered (`0` = off) |
| `clean_removed` | `true` | drop the state of files that no longer match the glob once they are drained and closed |
| `max_line_bytes` | `262144` | maximum line length in bytes (aligned with VictoriaLogs' `-insert.maxLineSizeBytes`; the buffer is capped, an oversized line never blows up memory) |
| `oversize` | `truncate` | `truncate` emits the first `max_line_bytes` of an oversized line (the decode/DLQ path makes it visible); `skip` drops the line and counts it |
| `multiline` | off | aggregate multi-line records (stack traces) into one message — see [Multiline](#multiline) |
| `host` | `os.Hostname()` | value carried as `meta.host` |

Every message carries `meta.file_path` (the matched path) and `meta.host`.
When the basename has the Kubernetes container-log shape
`<pod>_<namespace>_<container>-<id>.log`, the message additionally carries
`meta.pod`, `meta.namespace` and `meta.container` — parsed **automatically
from the path** (a regexp; no k8s API access, no configuration). Pod,
namespace and container names cannot contain underscores and the runtime id
is lowercase hex, so the parse is unambiguous; a host file that merely looks
like the shape just gets harmless extra fields, which is why the parse is
always on. Feed them to `fields.from_meta` (or a script) to make them payload
fields and to VictoriaLogs' `_stream_fields`:
`stream_fields: host,app,namespace,pod`.

Line and lifecycle semantics:

- **Only newline-terminated lines are emitted**; a half line at EOF waits in
  memory for its newline (changed in the P2 work — it used to be emitted
  immediately). `on_eof: stop` emits an unterminated final line as its last
  message, because a complete batch file may end without a newline.
- **Rotation** drains the old descriptor to EOF before opening the new file
  from `start_at`; **copytruncate** restarts at 0 (duplicates are acceptable,
  a stall is not); a **deleted** file is read to its end from the held
  descriptor.
- Idle descriptors are closed after `close_inactive_ms` and reopened on the
  next growth; a removed file's state is dropped by `clean_removed` once it is
  drained and closed.
- The per-file state is
  `{"version":2,"files":{"<id>":{"path":"...","offset":N}}}`; a v1
  `{"offset":N}` state is still applied to a single (meta-free) path.

### Multiline

Text logs (Java stacks, Go panics, Python tracebacks) need several lines per
event. `multiline` classifies every complete line with a Go regexp and joins
the group with `\n` into **one** message:

```yaml
sources:
  logs:
    decoder: raw               # a stack trace is not JSON; raw passes it through
    file:
      path: /var/log/app/*.log
      multiline:
        pattern: '^\S'         # matches group-START lines (a line starting
                               # at column 0: a timestamp or log level)
        negate: true           # ... so a negated hit is a continuation line
        match: after           # after = a (negated) hit continues the group
        max_lines: 500         # caps (defaults); exceeding one flushes the
        max_bytes: 1048576     # current group and starts a new one with the
        timeout_ms: 2000       # incoming line; timeout flushes a group when
                               # no new line arrived (0 = no timeout — the
                               # lint_multiline_no_timeout warning)
```

The equivalent `before` shape, where the pattern matches the first line of a
group and everything else continues it:

```yaml
      multiline:
        pattern: '^\d{4}-\d{2}-\d{2}'
        match: before
```

Rules to rely on:

- A group flushes on a new group-starting line, `timeout_ms`, `max_lines`,
  `max_bytes`, rotation/truncation and stop-mode EOF. It does **not** flush on
  shutdown: an uncommitted group is re-read from the watermark after a restart
  (duplicates, never loss).
- The watermark of a group is the end offset of its **last** line, so a
  restart never re-reads an emitted group.
- A group made only of whitespace lines is dropped at flush.
- A group never outlives its descriptor: closing an idle file
  (`close_inactive_ms`) flushes the open group first, so keep
  `close_inactive_ms` well above the inter-line gap for slow stack traces.
- `oversize: skip` drops the line **without** breaking the group;
  `oversize: truncate` joins the truncated content.
- Without `multiline` every line is one message, exactly as before.

The `fields` transform copies configured values into the payload map; the
payload must be a map (anything else is a typed transform failure, retried
then dead-lettered). Application order is payload copy, then `from_meta`
(missing keys are skipped), then `fields` — so an explicitly configured field
overrides both the payload and a meta-derived one. `${VAR}` and
`${constants.x}` substitution runs before the plugin sees the values.

The example ships a contract suite (`tests/collector.yaml`: the stamp, the
timestamp backfill, the malformed-line DLQ path) that `eventboat test` runs
against the real engine, and `internal/inttests/victorialogs` drives the same
shape into a live VictoriaLogs and queries the rows back through LogsQL
(gated by `EVENTBOAT_VICTORIALOGS_URL`).

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
  (`encoder: json`) and why the file source keeps `max_line_bytes` aligned
  with VL's `-insert.maxLineSizeBytes`: a raw-text body would lose lines
  silently.
- **4xx — permanent.** VL returns 4xx only when the whole batch is
  unparseable; the edge's delivery policy exhausts and the batch dead-letters.
  Re-sending cannot help.
- **5xx / network error / timeout — transient.** Retried per edge policy,
  then dead-lettered. VL pushes back instead of dropping while overloaded.

## Metrics

The file source exposes monotonic health counters through
`registry.CounterSource`. Every status snapshot polls them and writes the
**delta** since the previous snapshot to telemetry, so each counter surfaces
as `eventboat_source_<counter>_total{pipeline,node}` on `/metrics` (and
through the OTLP exporter when configured). A counter that goes backwards (a
source restart, a redeploy) re-baselines instead of writing a negative delta.

| Counter | Meaning |
|---|---|
| `lines_read` | complete lines parsed (blank lines and oversize lines included) |
| `lines_skipped` | lines dropped by `oversize: skip` |
| `lines_truncated` | lines emitted truncated by `oversize: truncate` |
| `rotations` | files replaced at a matched path (identity change: rename + recreate) |
| `multiline_merges` | continuation lines appended to an open multiline group |

Use them to size the knobs (design §2.6.2): a rising `lines_skipped` says
`max_line_bytes`/`oversize` needs attention, `rotations` with gaps in
`lines_read` says logrotate retention is too aggressive, and
`multiline_merges` shows how much the aggregator is actually merging.

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
  the unit template plus logrotate guidance — `create` mode is preferred;
  `copytruncate` rewrites the file in place and the tailer restarts at 0 on
  the next scan (duplicates, no stall; design §2.3).
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
