---
title: "Expressions & scripting sandbox"
order: 5
---

# Expressions & scripting sandbox

Eventboat hosts three languages, each with one job: **CEL** is the default
edge predicate dialect, **CESQL** is an opt-in CloudEvents-flavored
alternative for edges, and **Starlark** is the scripting language of the
built-in `script` transform. All three are sandboxed, deterministic and
side-effect free — a property `explain` relies on to dry-run pipelines, and
the reason replay produces the same answers as production. The WASM tier
covers compute-heavy transforms that outgrow Starlark.

## Edge predicates

An edge condition is declared on `from`:

```yaml
sinks:
  eu-out:
    from:
      - enrich: { when: 'payload.region == "eu"' }        # string form = CEL
      - warm:
          when: { lang: cesql, expr: "severity >= 5" }    # object form
```

The string form is CEL. The object form (`internal/config/sections.go`)
takes `lang: cel` (default) or `lang: cesql` plus a non-empty `expr`. Both
hosts implement one interface (`internal/ir/ir.go`, `WhenPredicate`) with
one error contract: **an evaluation error means the condition does not
pass**, plus a counter (`eventboat_cel_eval_errors_total`) — never a panic,
never a silent pass. A fan-out where no edge matches commits the message as
filtered.

## CEL host

`internal/lang/celhost` hosts CEL exactly as-is: no custom functions, no
syntax extensions (§4.2). The activation binds four dyn-typed variables
(`celhost.NewEnv` / `Predicate.evalValue`):

| Variable | Contents |
|---|---|
| `payload` | the decoded message payload |
| `meta` | engine-stamped + source metadata (`message_id`, `ingest_time`, `source`, route, ...) |
| `constants` | the pipeline's `constants:` map |
| `parameters` | job parameters (empty in continuous pipelines — references are rejected at verify) |

The same env compiles sink `order_key` expressions (result must be a
string).

**The cost limit.** Every predicate program is built with
`cel.CostLimit(1_000_000)` (`maxEvalCostUnits`). The rationale from the code:
the runtime cost model charges ~1 per arithmetic/logical step, per element
for comprehensions, and input length × 0.1 for size-driven operations
(equality/regex/contains), so a realistic predicate over a KB-scale message
lands in the low hundreds; 1e6 leaves an order of magnitude of headroom
while cancelling payload-driven blowups. Exceeding the limit surfaces as an
ordinary `EvalError` ("operation cancelled: actual cost limit exceeded") —
so a too-expensive predicate makes the **edge not match** and increments
`eventboat_cel_eval_errors_total`; it never aborts the message. Compile
errors (`CompileError`) are verify findings instead.

## CESQL dialect

`internal/lang/cesqlhost` hosts the opt-in CESQL dialect (§4.7) by reusing
the official CloudEvents SDK parser (`cloudevents/sdk-go/sql/v2`); the
official TCK is vendored (275 cases) and must stay green in CI. Dialect
rules, per the code:

- **Identifiers are alphanumeric only** — CESQL's lexer has no `.` token and
  no underscore, mirroring CloudEvents attribute names. Meta keys containing
  underscores (`kafka_offset`, `message_id`) are therefore *unreachable* in
  this dialect; use CEL for those.
- **`data.*` payload extension** is a string-literal-aware pre-parse rewrite
  to synthetic camelCase identifiers: `data.amount` → `dataAmount`,
  `data.a.b` → `dataAB` (`rewriteDataPaths`). Nested objects flatten up to
  depth 4; identifiers starting with `data` (case-insensitive) are reserved
  for the extension. Nulls are not injected (they read as missing
  attributes); arrays and non-scalar leaves become JSON strings.
- **Meta maps to context attributes**: `id`, `source`, `type`, `subject`,
  `time` route through the event writer (required context attributes);
  everything else becomes an extension. Values map string→string,
  bool→bool, integral numbers→int32, other numbers→decimal string,
  arrays/objects→JSON string.

The extension mode is "CESQL subset + documented extension" — never
advertised as full CESQL compatibility.

## Starlark sandbox

`internal/lang/starhost` runs `transforms.<node>.script` — a statement
sequence bound against `payload`, `meta`, `constants`, `parameters`.
Guarantees, all visible in `starhost.go`:

- **Syntax restrictions** (`fileOptions()`): `syntax.FileOptions` is built
  with `Set: true`, `TopLevelControl: true` (scripts are statement
  sequences, not function bodies — review R2), and **`While: false`,
  `Recursion: false`, `GlobalReassign: false`**. No `while` loops, no
  recursion, no global reassignment.
- **Step budget**: `thread.SetMaxExecutionSteps(100_000)` (`DefaultOptions`).
  Exhaustion kills the execution; the engine flags it (`"steps"`) and
  `eventboat_script_step_budget_exhausted_total` counts it. A script that
  exceeds the budget dead letters after the incoming edge's retries.
- **Load allowlist**: exactly `json` and `math` (`allowedModules`); there is
  no loadable `strings` module in go-starlark — string methods are built
  into the string type. No time, no I/O, no entropy: evaluation is
  deterministic and replayable.
- **Predeclared bindings**: `payload`, `meta`, `constants`, `parameters`
  (frozen), plus `safe_json_decode(s, default)` — the one sanctioned escape
  hatch, returning the default instead of aborting on bad JSON — and
  `remove(dict, key)`.
- **Per-message Thread**: each execution gets a fresh `starlark.Thread`
  (`RunWithParams`); payload/meta bind lazily with copy-on-write semantics,
  so a failed attempt's writes never leak into a retry.
- **Precise dirty tracking** (beta round): mutations mark through a marker
  threaded into the converted dicts, so only *written* subtrees materialize
  back into the message. Container *reads* no longer dirty; list-bearing
  trees dirty conservatively (native Starlark list mutators cannot be
  intercepted — the boundary is locked by a dedicated test in
  `internal/lang/starhost`, and a future list wrapper would flip exactly
  that test). After `Apply`, only dirty states are written back to the
  message (`internal/registry/builtin/transform_script.go`).
- **`fail()` behavior**: Starlark has no exceptions to swallow; a runtime
  failure becomes `ScriptError{Msg, Backtrace, Line}` (`asScriptError` —
  the innermost frame carries the user-visible line). The engine dead
  letters the message with the backtrace verbatim; positions render as
  `script:L:C`. Compiled programs are immutable, so one program serves all
  workers concurrently (script registers **without** `TransformCloner`).

## WASM transforms

The third tier (`internal/wasmhost`, guest ABI in `docs/wasm.md`) runs
wasip1 **reactor** modules under wazero. Guests build with the standard Go
toolchain (`GOOS=wasip1 go build -buildmode=c-shared`); the ABI is
`_initialize`, `eb_alloc(len)`, `transform(ptr, len)` (plus optional
`eb_last_error`). Wire format: payload as JSON bytes in/out; metadata passes
through untouched (chain a `script:` node to touch meta).

Resource model — wazero has no instruction metering, so budgets are:

- **Memory cap**: `max_memory_pages`, default 512 pages = 32 MiB
  (`DefaultMaxMemoryPages`), always on.
- **Wall-clock budget**: `timeout_ms` is **opt-in**. When set, wazero's
  `CloseOnContextDone` kills runaway guests — measured at ~5x throughput
  cost on loop-heavy guests. When unset (**fast mode, the default**) there
  is **no kill switch**: a runaway guest wedges its one worker until the
  pipeline restarts. This is a documented tradeoff (M3-audit J2 ruling): a
  tier whose reason to exist is performance cannot default to slower than
  Starlark; the risk is availability, not correctness — no data is lost and
  the seven invariants hold. Guardrails: the `wasm_no_kill_switch` verify
  warning (escalated by `--strict`), and the zero-interference slow-call
  watchdog (engine `WasmSlowCallWarnMs`, default 5000ms) logs a wedged
  invoke once.
- Traps/timeouts in protected mode kill the instance, which is transparently
  re-instantiated per worker (`TransformCloner` — module instances are not
  goroutine-safe). `explain` does not dry-run wasm guests.

## Which layer for which job

- **Edge predicate (`when:`)** for routing decisions: cheap, per-edge,
  evaluated before delivery. CEL first; CESQL only for CloudEvents
  interop or teams that already speak it.
- **`script:` transform** for mapping, enrichment, filtering-by-computation
  and routing via `meta.route`. The default choice — the ladder's trigger
  standard (§4.6) is performance or dependencies, never "complex logic".
- **`wasm:` transform** only when the ladder justifies it: measured heavy
  per-message computation (the shipped benchmark: heavy aggregation 2.5ms
  WASM vs 5.9ms Starlark, ~30,000x fewer allocations in fast mode) or a
  dependency that only exists as a guest. For light mapping Starlark wins
  outright.

## Scratch evaluation: repl and explain

Two tools run the same hosts outside a live pipeline:

- `eventboat repl --message sample.json --cel 'payload.total > 100'` (or
  `--script fix.star`) evaluates one expression/script against a sample;
  the interactive loop re-executes the accumulated script against the
  original sample each line — deterministic session semantics, the §4.3
  guarantee.
- `eventboat explain --message sample.json` really executes scripts and
  evaluates each outgoing edge against the *transformed* payload (the
  sandbox is deterministic and side-effect free, review R10), so the
  walkthrough shows the answer production would give. A failing script
  renders its backtrace and the dead-letter consequence. Without
  `--message`, nothing executes (symbolic summary only).

## The deterministic-replay caveat

The sandboxes are deterministic on purpose: no clocks, no randomness, no
I/O in Starlark; WASI provides fake clocks and no filesystem. This is what
lets `explain --message` execute scripts for real, lets `repl` re-execute a
session deterministically, and makes contract tests reproducible. The
caveat runs the other way too: **anything nondeterministic you need must
enter through the message** — stamps like `ingest_time` in `meta`, or
parameters resolved at trigger time — because the pipeline itself will
never draw entropy at run time, and a script that tried to (no such builtin
exists today) would break the replay guarantees the docs make.
