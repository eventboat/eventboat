# eventboat convert — v2 → v3 migration report

- **Source**: testdata/v2/03-fan-in.yaml (style: steps (YAML))
- **Statements**: 2 auto / 0 manual
- **Structural notes**: 2 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- cron source "events-source": 6-field schedule "0 */3 * * * *" → 5-field expression "*/3 * * * *" (v3 uses standard 5-field cron)
- cron source "orders-source": 6-field schedule "0 */2 * * * *" → 5-field expression "*/2 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "merge" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.processed = true` | `payload.processed = True` | auto |
| `metadata.source_stream = payload.stream` | `meta.source_stream = payload.stream` | auto |
