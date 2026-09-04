# eventboat convert — v2 → v3 migration report

- **Source**: ../../legacy/_examples/multi-pipeline/heartbeat.yaml (style: steps (YAML))
- **Statements**: 0 auto / 0 manual
- **Structural notes**: 1 · **Manual items**: 0
- **Verify (v3)**: PASS (0 errors, 0 warnings)

## Structural transformations

- cron source "tick": 6-field schedule "0 */10 * * * *" → 5-field expression "*/10 * * * *" (v3 uses standard 5-field cron)
