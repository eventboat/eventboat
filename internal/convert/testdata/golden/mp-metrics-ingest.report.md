# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/_examples/multi-pipeline/metrics-ingest.yaml (style: steps (YAML))
- **Statements**: 2 auto / 0 manual
- **Structural notes**: 1 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- cron source "sample": 6-field schedule "0 */15 * * * *" → 5-field expression "*/15 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "normalize" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.ts = "riverpod"` | `payload.ts = "riverpod"` | auto |
| `metadata.metric = true` | `meta.metric = True` | auto |
