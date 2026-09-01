# Riverpod Agent Reference

Supplement to [SKILL.md](SKILL.md). Read when configuring specific plugins or admin/observability endpoints. This file ships with the skill for standalone installs via [skills.sh](https://skills.sh/).

## Terminology

| Term | Layer | Meaning |
|------|-------|---------|
| step | config | `steps.{name}` block in YAML/HOCON |
| stage | runtime | `StageIR` after expansion (source/transform/sink) |
| edge | runtime | Directed link from `depends_on` → `EdgeIR` |

Combined step (`transform` + `sink` in one step) expands to `{name}` + `{name}-sink` with an automatic inner edge.

## HOCON equivalent

```hocon
engine { max_workers = 16, max_inflight = 10000 }

steps {
  cron-in {
    source {
      type = cron
      decoder = json
      config { schedule = "*/5 * * * * *" }
    }
  }
  enrich {
    depends_on = [cron-in]
    transform {
      type = map
      config { dsl = "payload.ts = format_time(now(), \"%Y-%m-%dT%H:%M:%SZ\")" }
    }
  }
  out {
    depends_on = [enrich]
    sink { type = drop }
  }
}
```

## Source plugins

### cron

```yaml
source:
  type: cron
  decoder: json
  config:
    schedule: "0 */1 * * * *"   # 6-field cron (with seconds)
    timezone: UTC
    payload: '{"event":"tick"}'  # optional static payload
```

### kafka

```yaml
source:
  type: kafka
  decoder: json
  config:
    brokers: ["${KAFKA_BROKERS}"]
    topics: [orders]
    group_id: my-consumer
```

### http_server

```yaml
source:
  type: http_server
  decoder: json
  config:
    address: ":8081"
    path: /webhook
```

## Transform plugins

### map / filter

```yaml
transform:
  type: map    # or filter
  workers: 4
  predicate: "metadata.region == 'us'"   # optional: skip Process when false
  error_mode: propagate   # propagate | ignore | silent
  config:
    dsl: |
      payload.normalized = lowercase(payload.email)
```

### route

```yaml
transform:
  type: route
  config:
    routes:
      high: "payload.priority == 'high'"
      _default: "true"
```

Writes matched route name to `metadata["er-route"]`.

## Sink plugins

### kafka

```yaml
sink:
  type: kafka
  encoder: json
  batch: { size: 100, timeout: 1s }
  ordering: ordered   # ordered | unordered
  config:
    brokers: ["${KAFKA_BROKERS}"]
    topic: output-topic
```

### http

```yaml
sink:
  type: http
  encoder: json
  config:
    url: https://api.example.com/events
    method: POST
    headers:
      Authorization: "Bearer ${API_TOKEN}"
```

## Edge attributes (depends_on value)

| Field | Purpose |
|-------|---------|
| `route` | Match `metadata['er-route']` from upstream route transform |
| `condition` | CEL boolean; mutually exclusive with `route` |
| `buffer` | `{ type: memory\|disk\|overflow, size, ... }` |
| `delivery.retry` | `{ max, backoff: fixed\|exponential, initial, max_interval }` |
| `delivery.dlq` | sink stage id for failed messages |
| `required` | `false` = best-effort edge (failure ignored for Ack) |

## Pipeline-level

```yaml
edgeDefaults:
  buffer: { type: memory, size: 64, strategy: block }

dlq:
  sink: dlq-kafka-sink
  include_current_payload: false

engine:
  max_workers: 16
  max_inflight: 10000
  error_mode: propagate
  drain_timeout: 30s
```

## Observability defaults

```yaml
observability:
  metrics:
    enabled: true
    port: 9090
    path: /metrics
  health:
    enabled: true
    port: 8080
    endpoints:
      liveness: /live
      readiness: /ready
```

## Admin API JSON shapes

**POST /admin/reload/{name}** → 202:

```json
{ "task_id": "...", "status": "accepted" }
```

**GET /admin/pipelines** → 200:

```json
{ "pipelines": ["order-processing", "..."] }
```

**GET /admin/reload/status/{task_id}** → task object with status field.

**409 reload conflict:**

```json
{ "error": "pipeline reload already in progress" }
```

## Example patterns (riverpod repo)

When cloning [riverpod/riverpod](https://github.com/riverpod/riverpod), see `_examples/`:

| File | Pattern |
|------|---------|
| 01-linear-etl.yaml | Linear cron → map → filter → drop |
| 02-route-branching.yaml | route + multi sink |
| 03-fan-in.yaml | Multiple sources → one transform |
| 04-http-webhook.yaml | http_server → http sink |
| 06-edge-delivery.yaml | buffer, retry, DLQ |

## Further reading (repo only)

- Full config spec: `docs/configurations.md`
- Agent roadmap: `docs/ai-agent.md`
