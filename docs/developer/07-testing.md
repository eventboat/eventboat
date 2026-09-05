---
title: "Testing guide"
order: 7
---

# Testing guide

Eventboat's testing philosophy: every reliability property is a named,
retrievable test; every externally visible surface (help screens, plugin
schemas) is pinned by a golden file; and anything that claims to work
against the real world (Kafka, long runs) is proven against the real thing
in CI. The full suite is `go test ./...`; CI runs it with `-race`.

## The test taxonomy

| Layer | Where | What it proves |
|---|---|---|
| Unit tests | beside the code (`*_test.go` per package) | package-level behavior: loader rules, schema generation, host evaluation, store semantics |
| Engine invariants | `internal/engine/invariants_test.go` | the seven §6.2 reliability invariants, one test each |
| Engine behavior | `internal/engine/*_test.go` | delivery, fan-out, split, transform plugins, wasm, CESQL, admission pooling |
| Persistence/recovery | `internal/engine/commit_persist_test.go`, `retention_test.go`, `internal/store` | flush ordering, retention bounds, crash replay |
| Golden tests | `cmd/eventboat/testdata/help/`, `internal/registry/builtin/testdata/schemas/` | CLI help screens; generated plugin JSON Schemas |
| Contract tests | `internal/testrun` + `examples/*/tests/` | pipeline behavior through the real engine via suite YAML |
| Integration | `internal/inttests/kafka`, `internal/inttests/soak` | real broker roundtrips; long-run stability (env-gated, need Docker) |
| Benchmarks | `internal/engine`, `internal/lang/*`, `internal/wasmhost` | performance gates and the Starlark-vs-WASM comparison |
| Acceptance | `cmd/eventboat/examples_test.go`, `internal/rpcplugin/acceptance_test.go`, `cmd/eventboat/agent_loop_test.go` | every example verifies and passes its suites; the third-party gRPC plugin builds and runs; the full agent loop over MCP stdio |

## The seven engine invariants

`internal/engine/invariants_test.go` pins redesign-v3.md §6.2 — these names
are load-bearing; do not rename them:

- `TestInvariant_SpoolBeforeVisible` — a failing spool append refuses the
  message; it never reaches a sink.
- `TestInvariant_CheckpointAdvancesOnlyAfterCommit` — the checkpoint trails
  committed messages.
- `TestInvariant_Kill9ReplayReplaysAllUncommitted` — crash recovery replays
  a superset of the uncommitted set.
- `TestInvariant_DeadLetterWriteFailureBlocksCommit` — a failing dead-letter
  write never drops the message.
- `TestInvariant_RequiredFalseFailureDoesNotBlockSiblingBranches` — optional
  edge failure isolates to its branch.
- `TestInvariant_RedeliveryKeepsMessageIdStable` — duplicate delivery keeps
  `meta.message_id`, so idempotent sinks can dedup.
- `TestInvariant_CursorWatermarkNeverExceedsCommitted` — pull-source
  watermarks only advance over committed messages.

**The `-race` policy**: the engine and store packages must stay race-clean —
CI runs `go test -race ./...` on every push. If you add concurrency to either
package, run the race detector locally first; a data race here is a correctness
bug in the durability model, not a hygiene issue.

## Golden tests

Three golden surfaces, each with its own regenerate flag:

- **CLI help snapshots**: `cmd/eventboat/testdata/help/*.txt` pin every verb's
  help screen. After changing a flag or usage string:
  `go test ./cmd/eventboat -update` and commit the diff.
- **Plugin schema goldens**: `internal/registry/builtin/testdata/schemas/*.json`
  pin the JSON Schema generated from every builtin config struct. After
  changing a config struct: `go test ./internal/registry/builtin
  -update-schemas`. The test failure text tells you the flag.
- **Protobuf descriptor golden**: `internal/registry/builtin/testdata/example.descr`,
  regenerated with `-update-descr` (the protobuf codec test's fixture; no
  protoc involved).

A golden diff in a PR must be reviewable line by line — that is the point.

## Integration tests (need Docker)

Both suites skip silently without their env var; CI sets it:

```bash
# real Kafka via testcontainers (KRaft, confluent-local):
# roundtrip through the engine, dead-letter path, consumer-group rebalance
EVENTBOAT_KAFKA_TEST=1 go test ./internal/inttests/kafka/ -v -timeout 8m

# long-run stability: mixed load + injected faults; asserts at-least-once,
# full checkpoint advance and no goroutine leaks
EVENTBOAT_SOAK_TEST=1 EVENTBOAT_SOAK_DURATION=2m go test ./internal/inttests/soak/
```

`EVENTBOAT_SOAK_DURATION` bounds the soak run (CI default 25m; nightly plus
manual dispatch via `.github/workflows/soak.yml`). The wasm tests optionally
honor `EVENTBOAT_WASM_GUEST` — CI rebuilds the guest from source and points
the env at the fresh artifact; locally the committed `.wasm` under
`internal/wasmhost/testdata/` is used, so plain `go test ./...` needs no
wasm toolchain.

## Benchmarks

```bash
bash scripts/bench-gate.sh   # what CI enforces
```

`scripts/bench-gate.sh` is a **loose order-of-magnitude gate**, not a noise
gate — limits sit ~5–15x above the reference dev machine (i5-14600KF) to
absorb shared-runner variance:

| Benchmark | Package | Gate |
|---|---|---|
| `BenchmarkPredicateEval` | `internal/lang/celhost` | ≤ 5000 ns/op |
| `BenchmarkSimpleScript` | `internal/lang/starhost` | ≤ 20000 ns/op |
| `BenchmarkContainerReadOnly` | `internal/lang/starhost` | ≤ 20000 ns/op |
| `BenchmarkCommitThroughput/mem` | `internal/engine` | ≤ 100000 ns/op (the 100µs gate) |

It covers the per-message hot paths (predicate eval, script exec, commit
throughput against the memory store) and deliberately does **not** cover
WASM (runner variance too high — `BenchmarkHeavyTransform` in
`internal/wasmhost` stays informational) or SQLite-backed throughput. What a
gate failure means: an algorithmic regression landed; profile before
optimizing.

## testkit and testrun

`internal/testkit` provides the deterministic primitives engine and contract
tests build on:

- `ManualSource` — a source whose `Emit` blocks until the engine accepted
  the message (spooled + dispatched), making tests deterministic;
  `Commit` records the cursor watermark like a real pull source would.
- `CaptureSink`/`Recorder` — capture delivered messages; `DiscardSink`;
  `FlakySink` — fault-inject sink writes by attempt count.
- `StoreWrapper` — hook `AppendSpool`/`WriteDeadLetter`/`SetCheckpoint` to
  inject store faults.
- `FixedClock`/`CounterID` — frozen clock and deterministic IDs.
- `RegisterFakeTransform`/`TransformFunc` — register a function as a
  transform plugin with an accept-anything schema (see
  [Plugins](03-plugins.md)).

`internal/testrun` is the contract-test runner: it builds the pipeline IR,
runs each case against a **real engine** with an ephemeral memory store,
fixed clock, capture-wrapped sinks (the plugin sink never touches the real
world), injects at the named node, waits for commit, and evaluates
expectations (in-order subset matching on `payload.*`/`meta.*` dotted paths,
DLQ count/reason). Job pipelines run with sources disabled so injections at
the pull node are deterministic.

### The suite format

The project convention is `<root>/pipeline.yaml` + `<root>/tests/<suite>.yaml`
(+ `tests/fixtures/`); the suite references its pipeline as
`pipeline: ../pipeline.yaml` (path resolution is contained within the suite
root — an escaping path is an error):

```yaml
suite: order-branching
pipeline: ../pipeline.yaml
cases:
  - name: eu-order-routed-to-eu-only
    inject: { at: ingest, messages: [fixtures/eu-order.json] }
    expect:
      capture:
        at: eu-out
        messages:
          - payload.total: 12000     # subset match
            meta.tier: vip
  - name: malformed-json-to-dlq
    inject: { at: ingest, raw: "{not json" }
    expect:
      dlq: { count: 1, reason_contains: decode }
```

`eventboat test <path>` consumes it: a file recurses (`.yaml` files without
a top-level `suite:` key are skipped and counted; unreadable/unparseable
YAML is a hard error, ruling D3). Exit codes: failing cases → 1; a directory
with no valid suites but broken files → 2 (ruling D4).

## Practical recipes

**Run the fast loop** (seconds, no Docker):

```bash
go build ./... && go test ./... 
```

**Run everything locally before a PR:**

```bash
go test -race ./...                          # full suite, race on (engine+store policy)
bash scripts/bench-gate.sh                   # perf gate
EVENTBOAT_KAFKA_TEST=1 go test ./internal/inttests/kafka/   # needs Docker
```

**Add a regression test for a delivery bug**: reproduce with the smallest
pipeline YAML (see `invYAML` in `invariants_test.go` for the minimal
shape), drive it with `testkit.ManualSource` + `CaptureSink` +
`StoreWrapper` fault hooks, assert on `Recorder.Captured()` or the store's
dead letters. If it touches commit/checkpoint ordering, check whether one of
the seven invariants already covers the class — extend that test's scenario
instead of asserting something weaker.

**Add a contract test for a user-visible pipeline behavior**: write the
suite under `examples/<pipeline>/tests/`, fixtures under `tests/fixtures/`,
and rely on the CI examples gate (`cmd/eventboat/examples_test.go`) to keep
it green.
