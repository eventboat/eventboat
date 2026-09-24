# Documentation index

Three groups of documents live under `docs/`. The rendered site
(`tools/sitegen`) covers the developer guides and the reference pages; the
design archive is repo-only.

## Developer guides — `docs/developer/`

The numbered tour of how the system works and how to change it, rendered as
the documentation site's guide section.

| # | Guide |
|---|---|
| 1 | [Architecture & package map](developer/01-architecture.md) |
| 2 | [Engine internals](developer/02-engine.md) |
| 3 | [Plugin system & registry](developer/03-plugins.md) |
| 4 | [Configuration & diagnostics](developer/04-config-pipeline.md) |
| 5 | [Scripting](developer/05-scripting.md) |
| 6 | [Observability](developer/06-observability.md) |
| 7 | [Testing](developer/07-testing.md) |
| 8 | [Building & deployment](developer/08-building.md) |
| 9 | [Contributing](developer/09-contributing.md) |

## Reference pages — `docs/`

Rendered as the site's reference section (except the internal naming
checklist, which is repo-only).

- [Plugins & the gRPC protocol](plugins.md)
- [Codecs](codecs.md)
- [WASM transforms](wasm.md)
- [Kubernetes deployment](k8s.md)
- [Collecting logs (files → VictoriaLogs)](collector.md)
- [Naming checklist](naming-checklist.md) — internal working notes

## Design archive — `docs/design/`

Design documents are archival: state lives in the document header, never in
the filename, and superseded documents stay. When a design is implemented,
update its status to `Implemented` with the commit reference; when a
decision is revisited, add the successor document to its `Links` row.

| Date | Title | Status | Summary |
|---|---|---|---|
| 2026-09-23 | [Architecture deepening program](design/2026-09-23-architecture-deepening.md) | Implemented (S1–S7) | Eight deepening candidates from the 2026-09-23 review: engine admission/replay, run outcomes, store ownership, verify path, framework vocabulary, failure kinds, jobs lifecycle, explain — with decisions, rationale, staged plans and acceptance tests. |
| 2026-09-24 | [Log collection mode — VictoriaLogs, file-tail hardening, tuning surface](design/2026-09-24-log-collection.md) | Draft | One-selection log collection for a file-based (host + container) estate: a batched VictoriaLogs sink, group-commit spool writes, file-source rotation/glob/multiline hardening, deployment shapes, and the performance tuning surface with sizing rules and guardrails. |

The domain vocabulary these documents rely on lives in
[`CONTEXT.md`](../CONTEXT.md) at the repo root.
