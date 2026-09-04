# eventboat convert — v2 → v3 migration report

- **Source**: testdata/v2/05-transform-sink-combined.yaml (style: steps (YAML))
- **Statements**: 2 auto / 0 manual
- **Structural notes**: 1 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- cron source "cron-source": 6-field schedule "0 0 * * * *" → 5-field expression "0 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "enrich" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.message = payload.message + " riverpod"` | `payload.message = payload.message + " riverpod"` | auto |

## Statement conversions — transform "publish" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `metadata.published = true` | `meta.published = True` | auto |
