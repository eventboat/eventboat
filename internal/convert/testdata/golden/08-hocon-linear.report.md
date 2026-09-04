# eventboat convert — v2 → v3 migration report

- **Source**: testdata/v2/08-hocon-linear.conf (style: steps (HOCON))
- **Statements**: 2 auto / 0 manual
- **Structural notes**: 2 · **Manual items**: 2
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- filter transform "filter-high" folded into its outgoing edges' `when` guards
- cron source "cron-source": 6-field schedule "0 */5 * * * *" → 5-field expression "*/5 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "enrich" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.total = payload.price * payload.quantity` | `payload.total = payload.price * payload.quantity` | auto |
| `metadata.enriched_at = "riverpod"` | `meta.enriched_at = "riverpod"` | auto |

## Manual items

### [M1] engine.max_workers

- **Reason**: v3 has no global worker cap; concurrency is per-node `workers`
- **Suggestion**: set `workers:` on the transform nodes that need it (default 1)

### [M2] source "cron-source" timezone Asia/Shanghai

- **Reason**: the v3 cron source has no timezone knob; it ticks in the host's local time
- **Suggestion**: run the eventboat process (or container) in the target timezone
