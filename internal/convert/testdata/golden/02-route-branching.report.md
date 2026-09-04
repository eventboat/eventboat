# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/_examples/02-route-branching.yaml (style: steps (YAML))
- **Statements**: 1 auto / 0 manual
- **Structural notes**: 3 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- route transform "splitter" folded into ordered edge guards (first-match semantics preserved)
- edge tag-region -> splitter carried attributes into folded gate "splitter"; those attributes no longer apply (the gate transform's own retry policy vanished with it)
- cron source "cron-source": 6-field schedule "0 */1 * * * *" → 5-field expression "*/1 * * * *" (v3 uses standard 5-field cron)

## Statement conversions — transform "tag-region" (map dsl)

| v2 (eql) | v3 (starlark/cel) | status |
|---|---|---|
| `metadata.region = payload.region` | `meta.region = payload.region` | auto |
