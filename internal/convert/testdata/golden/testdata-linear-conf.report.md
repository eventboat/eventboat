# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/testdata/pipelines/linear.conf (style: steps (HOCON))
- **Statements**: 1 auto / 0 manual
- **Structural notes**: 1 · **Manual items**: 2
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- filter transform "filter-high" folded into its outgoing edges' `when` guards

## Statement conversions — transform "enrich" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.total = payload.price * payload.quantity` | `payload.total = payload.price * payload.quantity` | auto |

## Manual items

### [M1] engine.max_workers

- **Reason**: v3 has no global worker cap; concurrency is per-node `workers`
- **Suggestion**: set `workers:` on the transform nodes that need it (default 1)

### [M2] sink "kafka-sink" ordering "ordered"

- **Reason**: v3 dropped the global ordered switch for per-key ordering
- **Suggestion**: set `order_key:` on a business key (e.g. 'payload.order_no') for partition-level ordering
