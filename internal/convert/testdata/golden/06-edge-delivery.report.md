# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/_examples/06-edge-delivery.yaml (style: steps (YAML))
- **Statements**: 1 auto / 0 manual
- **Structural notes**: 2 · **Manual items**: 2
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- v2 `dlq:` section and its target sink "dlq-sink" dropped: v3 dead letters go to the built-in dead-letter store (`eventboat replay --dlq`)
- cron source "cron-source": 6-field schedule "0 */1 * * * *" → 5-field expression "*/1 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "enrich" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `payload.enriched = true` | `payload.enriched = True` | auto |

## Manual items

### [M1] buffer.strategy

- **Reason**: buffer strategy "drop_newest" has no v3 equivalent (memory buffers block; the spool absorbs surges)
- **Suggestion**: for best-effort edges use `required: false` so failures drop instead of blocking

### [M2] delivery.dlq

- **Reason**: per-edge dlq target "dlq-sink" has no v3 equivalent
- **Suggestion**: v3 dead letters land in the built-in store; query/replay with `eventboat replay --dlq`
