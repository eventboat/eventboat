# eventboat convert — v2 → v3 migration report

- **Source**: testdata/v2/07-flat-pipeline.yaml (style: pipeline[] (YAML))
- **Statements**: 1 auto / 0 manual
- **Structural notes**: 2 · **Manual items**: 1
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- filter transform "keep-positive" folded into its outgoing edges' `when` guards
- cron source "cron-source": 6-field schedule "0 0 * * * *" → 5-field expression "0 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "double" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.value = payload.value * 2` | `payload.value = payload.value * 2` | auto |

## Manual items

### [M1] engine.max_workers

- **Reason**: v3 has no global worker cap; concurrency is per-node `workers`
- **Suggestion**: set `workers:` on the transform nodes that need it (default 1)
