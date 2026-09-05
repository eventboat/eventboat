---
title: Home
layout: index
---

Eventboat is a single Go binary: events come in (Kafka / HTTP / cron / file),
flow through an explicit DAG (filter, map, route), and land at their
destinations — at-least-once, verifiable, replayable. Predicates are plain
[CEL](https://github.com/google/cel-go) (the Kubernetes expression language),
transforms are [Starlark](https://github.com/google/starlark-go) (a Python
dialect): zero custom language to learn, maximal training corpus for the
agents that write your pipelines.

<ul class="cards">
  <li><a href="01-architecture.html"><strong>Architecture &amp; package map</strong><span>The data plane end to end: packages, boundaries, data flow.</span></a></li>
  <li><a href="02-engine.html"><strong>Engine internals</strong><span>Spool, commit tracking, checkpoints, crash recovery.</span></a></li>
  <li><a href="03-plugins.html"><strong>Plugin system &amp; registry</strong><span>Schemas, ABI versions, out-of-process gRPC plugins.</span></a></li>
  <li><a href="04-config-pipeline.html"><strong>Configuration &amp; diagnostics</strong><span>YAML sections, <code>from</code> edges, substitution, verify.</span></a></li>
  <li><a href="05-scripting.html"><strong>Expressions &amp; scripting sandbox</strong><span>CEL predicates, Starlark transforms, budgets.</span></a></li>
  <li><a href="06-observability.html"><strong>Observability &amp; operations</strong><span>Metrics, admin API, MCP, explain and replay.</span></a></li>
  <li><a href="07-testing.html"><strong>Testing guide</strong><span>Contract tests: fixtures, injection, capture, dead letters.</span></a></li>
  <li><a href="08-building.html"><strong>Building &amp; release</strong><span>Build the binary, WASM guest, release discipline.</span></a></li>
  <li><a href="09-contributing.html"><strong>Contributing</strong><span>Workflow, code style, spec discipline, the PR checklist.</span></a></li>
</ul>

## Why Eventboat

- **One binary, no runtime dependencies.** Sources, engine, sinks, admin UI
  and the agent tooling ship in one static Go executable — no broker
  required to start, no sidecars to operate.
- **Pipelines as code, verified by machines.** Every config passes
  `eventboat verify`: plugin JSON Schemas, topology rules, CEL + Starlark
  compilation and lint — statically, deterministically, before anything runs.
- **CEL predicates + Starlark transforms.** The two languages your team and
  your agents already know; no bespoke DSL, and contract tests
  (`eventboat test`) prove pipeline behavior against the real engine.
- **At-least-once with receipts.** Durable spool before the DAG sees a
  message, commit tracking to terminal states, crash replay, dead letters
  with backtraces, and `eventboat replay` to reinject or prune.
- **A real plugin registry.** In-process builtins and out-of-process gRPC
  source/sink plugins in any language, with ABI versions, `version:` pins
  and WASM transforms under a capability sandbox.
- **Agent-native operations.** `verify` / `test` / `explain` / `replay`,
  an LSP for the YAML, and an MCP server (14 tools) — plus `--json`
  everywhere, because operators are often not human.

## Try it

```bash
go install github.com/eventboat/eventboat/cmd/eventboat@latest

# gate 1: verify (static, deterministic, zero side effects)
eventboat verify --config examples/linear/pipeline.yaml

# gate 2: contract tests — in-process real engine, fixture injection, capture
eventboat test examples

# run (durable: SQLite store under ./data; or --ephemeral for local dev)
eventboat run --config examples/linear/pipeline.yaml
```

A minimal pipeline — three sections joined by `from` (`examples/linear`):

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: linear-etl }

constants:
  currency: EUR

sources:
  ingest:
    decoder: json
    file:
      path: input/orders.jsonl

transforms:
  enrich:
    from: [ingest]
    workers: 2
    script: |
      payload.total = payload.price * payload.qty
      payload.currency = constants.currency
      meta.label = "order-%s" % payload.id

sinks:
  out:
    from: [enrich]
    encoder: json
    batch: { size: 10, timeout_ms: 500 }
    file:
      path: output/orders.out.jsonl
```

## Status

**v0.1.0-beta** (spec v1.19): milestones M1–M4 plus the beta hardening round
are done; see the
[CHANGELOG](https://github.com/eventboat/eventboat/blob/main/CHANGELOG.md)
for exactly what shipped and what moved. The full design lives in
[redesign-v3.md](https://github.com/eventboat/eventboat/blob/main/redesign-v3.md);
中文说明见 [README_ZH](https://github.com/eventboat/eventboat/blob/main/README_ZH.md).
