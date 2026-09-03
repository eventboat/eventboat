# Riverpod

English | [中文](README_ZH.md)

A Go-based DAG event router. Built around a generic `Message` (`[]byte` payload + metadata), it processes streaming data through a **Source → Transform → Sink** directed acyclic graph, with conditional routing, per-edge buffering and delivery policies, retry/DLQ, and at-least-once Ack.

> **Current status:** v2.0-alpha observability is complete; now in **v2.0-beta** (Sprint 2+). See the [development roadmap](riverpod-design.md#12-开发路线图).

## Features

| Capability | Description |
|------------|-------------|
| **DAG topology** | Branching, fan-in/fan-out; edges declared via `depends_on` (including `route` / `buffer` / `delivery`) |
| **Protocol-agnostic** | The engine does not parse payloads; encoding/decoding is handled by Codec plugins (Source `decoder` / Sink `encoder`) |
| **eql** | CEL expressions plus assignment extensions (`payload.x = expr`, `del()`) for map / filter / edge conditions |
| **Reliability** | Automatic backpressure propagation, refCount Ack, edge-level retry/DLQ, optional disk buffer |
| **Observability** | `riverpod_*` Prometheus metrics, health checks; OTLP tracing and notifications planned |
| **Extensibility** | Go compile-time registration; WASM (wazero) and gRPC out-of-process plugins planned for v2.1 |
| **AI / Agent** | **Agent-first:** [skills.sh skill](skills/riverpod/SKILL.md) (`npx skills add riverpod/riverpod@riverpod`) + CLI/Admin API; in-pipeline `llm`/`agent` transforms planned v2.1+ — [AI/Agent Guide](docs/ai-agent.md) |
| **Deployment** | Single binary, multiple Pipelines; K8s Operator planned for v2.1 |
| **Configuration** | **YAML** (CRD / CI) + **HOCON** (Envelope-style local config), parsed into a unified IR; see [Configuration Reference](docs/configurations.md) |

## Architecture

```
YAML / HOCON / CRD  →  PipelineConfig  →  TopologyIR  →  Engine (fanOut/fanIn/Ack)
                              ↓
                    Source / Transform / Sink / Codec plugins
```

**Terminology:** config-layer **step** (`steps.{name}`) → runtime **stage** (`StageIR`) → **edge** (expanded from `depends_on`).

## Configuration Example

Recommended `steps` style (YAML):

```yaml
apiVersion: riverpod/v1
kind: Pipeline
metadata:
  name: order-processing

steps:
  kafka-in:
    source:
      type: kafka
      decoder: json
      config:
        brokers: ["${KAFKA_BROKERS}"]
        topics: [orders]

  enrich:
    depends_on: [kafka-in]
    transform:
      type: map
      config:
        dsl: |
          payload.total = payload.price * payload.quantity

  orders-out:
    depends_on: [enrich]
    sink:
      type: kafka
      encoder: json
      batch: { size: 100, timeout: 1s }
      config:
        topic: orders-enriched
```

Branch routing is declared on downstream steps via `route` in `depends_on`:

```yaml
  us-sink:
    depends_on:
      splitter: { route: us }
    sink:
      type: http
      config: { url: https://us-api.example.com/orders }
```

### Agent-first AI integration

**Step 1** is letting AI agents *operate* riverpod (write configs, validate, test, run) — not embedding LLM inside pipelines yet.

The repo ships an [Agent Skill](skills/riverpod/SKILL.md) compatible with [skills.sh](https://skills.sh/):

```bash
npx skills add riverpod/riverpod@riverpod
```

It teaches agents the CLI workflow, plugin catalog, and `depends_on` patterns. Example agent loop:

```bash
riverpod validate --config my-pipeline.yaml
riverpod test --config testdata/tests/my-suite.yaml
riverpod run --config my-pipeline.yaml
```

In-pipeline LLM classification (future `llm` transform + `route`) will build on the same DAG model — see [AI/Agent Guide](docs/ai-agent.md).

```yaml
# Planned v2.1+ — not available yet
  llm-classify:
    transform:
      type: llm
      config:
        provider: openai
        model: gpt-4o-mini
        messages:
          - role: user
            content: "Classify: ${payload.body}"
```

Equivalent HOCON, flat `pipeline[]` compatibility, and branch routing details are in the [Configuration Reference](docs/configurations.md); design background in the [Configuration Model](riverpod-design.md#8-配置模型).

## Repository Layout

| Path | Description |
|------|-------------|
| [`docs/configurations.md`](docs/configurations.md) | **Configuration reference** (Engine / Steps / plugins / edges / variable substitution) |
| [`docs/ai-agent.md`](docs/ai-agent.md) | **AI/Agent guide** (Agent Skill, MCP roadmap, in-pipeline LLM phases) |
| [`skills/riverpod/`](skills/riverpod/) | **Agent Skill** ([skills.sh](https://skills.sh/riverpod/riverpod/riverpod)) for pipeline authoring |
| [`skills/README.md`](skills/README.md) | Skills catalog and install commands |
| [`riverpod-design.md`](riverpod-design.md) | Requirements and design (primary document) |
| [`competitor-research.md`](competitor-research.md) | Competitive analysis |
| [`design-review.md`](design-review.md) | Design review (some entries outdated; primary doc takes precedence) |
| [`redesign-v3.md`](redesign-v3.md) | **v3 from-scratch redesign proposal** (agent-native router, zero-custom-language: CEL + Starlark; proposal, not yet implemented) |
| [`cmd/riverpod/`](cmd/riverpod/) | CLI (`run` / `validate` / `test`) |
| [`internal/`](internal/) | Engine, config, topology, eql |
| [`plugins/`](plugins/) | Source / Transform / Sink / Codec plugins |
| [`_examples/`](_examples/) | Demo configs for common patterns (linear ETL, branching, fan-in, edge policies, etc.) |
| [`testdata/pipelines/`](testdata/pipelines/) | Minimal configs for CI / unit tests |

## Build & Verify

```bash
go test ./...
go build -o bin/riverpod ./cmd/riverpod
bin/riverpod validate --config testdata/pipelines/linear.yaml
bin/riverpod test --dir testdata/tests
```

Or use the Makefile:

```bash
make build test validate pipeline-test
```

## Documentation

- [Configuration Reference](docs/configurations.md) — Engine, Steps, Source/Transform/Sink plugins, edges, and variable substitution (Envelope-style layout)
- [AI/Agent Guide](docs/ai-agent.md) — LLM/Agent transforms, provider abstraction, observability, phased delivery plan
- [Agent Skill (skills.sh)](skills/README.md) — install with `npx skills add riverpod/riverpod@riverpod`
- [v3 Redesign Proposal](redesign-v3.md) — agent-native positioning, zero-custom-language (CEL + Starlark), config & architecture redesign (proposal, not implemented)
- [Background & Goals](riverpod-design.md#1-背景与目标)
- [Core Interfaces & Message](riverpod-design.md#4-核心接口与-message-模型)
- [Engine Runtime](riverpod-design.md#6-引擎运行时)
- [eql DSL](riverpod-design.md#7-dsl-语言设计-eql)
- [Configuration Model (Design)](riverpod-design.md#8-配置模型)
- [v2.0 Finalization Checklist](riverpod-design.md#13-v20-定稿检查清单)

## Relationship to v1

riverpod is **not** backward compatible with Riverpod v1. v1 used a linear `Input → Processor → Output` model with CloudEvents binding; v2 is a ground-up redesign with DAG topology, generic Message, CEL/eql, Codec system, and dual deployment modes.

## License

TBD (to be determined during implementation).
