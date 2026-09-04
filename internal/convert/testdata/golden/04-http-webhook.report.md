# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/_examples/04-http-webhook.yaml (style: steps (YAML))
- **Statements**: 2 auto / 0 manual
- **Structural notes**: 1 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- http_server source "webhook-in": address → listen (same ":port" value)

## Statement conversions — transform "enrich" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `metadata.received_via = "webhook"` | `meta.received_via = "webhook"` | auto |
| `payload.received = true` | `payload.received = True` | auto |
