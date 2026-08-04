---
name: edgestream
description: >-
  Operate and author EdgeStream DAG event pipelines. Use when the user asks to
  create, validate, test, run, debug, or reload EdgeStream pipelines; write steps/
  depends_on YAML or HOCON; configure source/transform/sink plugins or eql;
  or work in the edgestream repository.
license: TBD
metadata:
  author: edgesets
  repository: https://github.com/edgesets/edgestream
  homepage: https://github.com/edgesets/edgestream
  skills-sh: edgesets/edgestream/edgestream
---

# EdgeStream Agent Skill

EdgeStream is a Go DAG event router: **Source → Transform → Sink**, with `depends_on` edges, eql (CEL + assignment), retry/DLQ, and at-least-once Ack.

**Your job:** help users design pipelines, write config, validate, test, and run — using CLI first, Admin HTTP API when the engine is already running.

## Install

```bash
npx skills add edgesets/edgestream@edgestream
```

Browse: [skills.sh/edgesets/edgestream/edgestream](https://skills.sh/edgesets/edgestream/edgestream)

## When to apply this skill

- User mentions edgestream, EdgeStream, event router, pipeline YAML/HOCON
- Creating or editing `steps`, `depends_on`, eql `dsl`, or plugin config
- Running `validate` / `test` / `run` / reload
- Debugging topology, routing, backpressure, or DLQ behavior

## Required reading (in order)

**Always read first:** [reference.md](reference.md) — plugin schemas, edges, admin API (bundled with this skill).

**When working inside the edgestream git repo**, also read:

1. [docs/configurations.md](../../docs/configurations.md) — full config reference
2. [_examples/](../../_examples/) — copy patterns before inventing new topology
3. [docs/ai-agent.md](../../docs/ai-agent.md) — agent integration roadmap
4. [edgestream-design.md](../../edgestream-design.md) — deep design when semantics are unclear

## Agent workflow

Always follow this loop when authoring pipelines:

```
1. Clarify intent (sources, transforms, sinks, routing, reliability)
2. Pick closest _examples/ template (or reference.md patterns if repo unavailable)
3. Write or edit config (prefer steps + depends_on)
4. edgestream validate --config <file>     # must pass before run
5. edgestream test --config <suite.yaml>   # when behavior must be proven
6. edgestream run --config <file>          # local run; cron+drop needs no deps
7. POST /admin/reload/{name}           # only if engine already running
```

**Rules:**

- Run `validate` after every config change; fix errors before suggesting run
- Prefer `steps.{name}` over flat `pipeline[]` unless user asks for flat style
- Declare edges only via downstream `depends_on` (no top-level `edges:`)
- Use `${ENV_VAR}` for secrets; never hardcode credentials
- For local demos without Kafka: `cron` source + `drop` sink

## CLI reference

Build once if needed: `go build -o bin/edgestream ./cmd/edgestream`

| Command | Purpose |
|---------|---------|
| `edgestream validate --config FILE` | Parse + topology check; exit non-zero on error |
| `edgestream validate --config-dir DIR` | Validate all `.yaml`/`.yml`/`.conf`/`.hocon` in dir |
| `edgestream test --config SUITE.yaml` | Fixture-driven pipeline test |
| `edgestream test --dir testdata/tests` | Run all suite files in directory |
| `edgestream run --config FILE` | Start engine (SIGINT/SIGTERM graceful stop; SIGHUP reload all) |
| `edgestream run --config-dir DIR` | Load multiple pipelines |

Makefile shortcuts: `make build`, `make test`, `make validate`, `make pipeline-test`

## Admin HTTP API (running engine)

Default health/metrics port from config (`observability.health.port`, often `8080`).

| Method | Path | Action |
|--------|------|--------|
| GET | `/admin/pipelines` | List pipelines |
| GET | `/admin/pipelines/{name}/status` | Pipeline status |
| POST | `/admin/reload` | Reload all (202 + task_id) |
| POST | `/admin/reload/{name}` | Reload one pipeline |
| GET | `/admin/reload/status/{task_id}` | Poll reload task |
| GET | `/ready` | Readiness (503 if any stage unhealthy) |
| GET | `/live` | Liveness |

409 on concurrent reload of the same pipeline.

## Config skeleton (YAML)

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: my-pipeline

engine:
  max_workers: 16
  max_inflight: 10000

steps:
  source-step:
    source:
      type: cron          # kafka | http_server | cron
      decoder: json       # json | raw
      config: { schedule: "*/1 * * * * *" }

  enrich:
    depends_on: [source-step]
    transform:
      type: map
      config:
        dsl: |
          payload.ingested_at = format_time(now(), "%Y-%m-%dT%H:%M:%SZ")

  out:
    depends_on: [enrich]
    sink:
      type: drop          # kafka | http | drop
```

### Branch routing

```yaml
  splitter:
    depends_on: [upstream]
    transform:
      type: route
      config:
        routes:
          us: "payload.region == 'us'"
          _default: "true"

  us-out:
    depends_on:
      splitter: { route: us }
    sink:
      type: http
      config: { url: "https://example.com/us" }
```

## Built-in plugins (v2.0-beta)

| Kind | type | Notes |
|------|------|-------|
| Source | `kafka`, `http_server`, `cron` | Kafka needs brokers; cron needs no deps |
| Transform | `map`, `filter`, `route`, `wasm` | wasm is stub — do not rely in prod |
| Sink | `kafka`, `http`, `drop` | `drop` for tests |
| Codec | `json`, `raw` | Source `decoder` / Sink `encoder` |

Full field schemas: [reference.md](reference.md).

## eql quick reference

Used in `map`/`filter` `config.dsl` and edge conditions.

```eql
payload.total = payload.price * payload.quantity
payload.tier = if payload.total > 1000 { "high" } else { "low" }
del(payload._internal)
payload.total > 100 && metadata.region == "us"
```

Variables: `payload`, `metadata`, `input` (read-only snapshot).

## Test suite format

```yaml
name: my-suite
pipeline: ../testdata/pipelines/linear.yaml
cases:
  - name: adds field
    input:
      - payload: { price: 10, quantity: 2 }
    expect:
      count: 1
      messages:
        - payload: { price: 10, quantity: 2, total: 20 }
```

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Missing `depends_on` on non-source step | Every transform/sink needs upstream edge |
| Both `route` and `condition` on same edge | Use one only |
| `payload.*` in eql without `decoder: json` | Add decoder or use raw bytes |
| Fan-out from one transform | Each downstream step declares its own `depends_on` + `route` |
| Editing without validate | Always run `edgestream validate` |

## Out of scope (not implemented yet)

- `llm` / `embed` / `agent` transforms — planned v2.1+
- MCP server for edgestream — planned Phase 1b
- Do not invent plugin types not listed above

## Extended reference

Plugin fields, engine options, HOCON examples: [reference.md](reference.md)
