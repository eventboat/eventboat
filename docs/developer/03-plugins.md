---
title: "Plugin system & registry"
order: 3
---

# Plugin system & registry

Every source, sink, codec and transform in eventboat is a plugin registered
in a `registry.Registry` (`internal/registry/registry.go`). Registration
carries a JSON Schema; configuration blocks are validated strictly against it
(unknown fields are errors), which is the mechanism that keeps agents from
inventing plugins or fields that do not exist (redesign-v3.md §5.6). The
registry is a leaf package: it imports nothing internal, so any Go program —
including your own build of eventboat — can register plugins into it.

## The four kinds

| Kind | Interface | Factory signature |
|---|---|---|
| `sources` | `Source` (+ optional `PullSource`) | `func(cfg map[string]any) (Source, error)` |
| `transforms` | `Transform` (+ optional `TransformCloner`, `TransformFlavor`) | `func(cfg any, dir string) (Transform, error)` |
| `sinks` | `Sink` | `func(cfg map[string]any) (Sink, error)` |
| `codecs` | `Codec` | `func(cfg map[string]any, dir string) (Codec, error)` |

Registration also pins an integer **ABI version** (builtins are version 1)
and optional capabilities (e.g. `"pull"` for sources, `"explain-safe"` for
transforms).

### Plugin-name-as-key

In YAML, the plugin name *is* the key of a node's block; everything else at
node level must be a framework field from the per-section whitelist
(`internal/config/sections.go`):

```yaml
sources:
  ingest:
    decoder: json              # framework field
    file: { path: input.jsonl }  # "file" is the plugin name
transforms:
  enrich:
    from: [ingest]             # framework field
    script: |                  # "script" is the plugin name
      payload.x = 1
sinks:
  out:
    from: [enrich]
    encoder: json
    file: { path: out.jsonl }
```

Exactly one plugin key per node (`cfg_missing_plugin` /
`cfg_multiple_plugins` otherwise). Names colliding with framework fields
(`from`, `decoder`, `encoder`, `workers`, `order_key`, `batch`, `when`,
`route`, `buffer`, `delivery`, `required`) are rejected at registration
(registry review R5). A `version:` pin on the node is checked against the
registered/manifest version at verify (`plugin_version_mismatch`).

## The typed schema system

Hand-writing a schema string and a `map[string]any` factory invites drift.
The typed layer (`internal/registry/typed.go`) derives both from one struct:

```go
type kafkaSourceConfig struct {
    Brokers []string `json:"brokers" schema:"minItems=1,desc=Kafka broker addresses"`
    Topics  []string `json:"topics" schema:"minItems=1"`
    GroupID string   `json:"group_id" schema:"default=eventboat"`
}

registry.RegisterSourceT(reg, "kafka", 1, nil, func(c kafkaSourceConfig) (registry.Source, error) {
    return newKafkaSource(c), nil
})
```

- `json` tags name the config keys; `schema` tags carry the constraint
  grammar: `optional`, `default=`, `enum=a|b|c`, `min=`, `max=`, `minLen=`,
  `minItems=`, `maxItems=`, `desc=` (must come last so descriptions may
  contain commas).
- Fields are required by default; `additionalProperties: false` is emitted
  always; properties render in declaration order.
- **Defaults are injected**, not just documented: the same `default=` tag is
  applied to zero-valued fields after decode, recursing into nested structs
  and slices of structs — schema-documented defaults are the runtime
  behavior.
- Generated schemas compile through the same santhosh-tekuri pipeline as
  hand-written ones, so `plugin_schema` diagnostics and the external gRPC
  manifest path are byte-compatible.
- **Golden tests** pin every builtin schema:
  `internal/registry/builtin/testdata/schemas/*.json`; regenerate with
  `go test ./internal/registry/builtin -update-schemas` after changing a
  config struct. Every schema change becomes a reviewable diff.
- The string-based `RegisterSource`/`RegisterSink`/`RegisterCodec`/
  `RegisterTransform` API remains as the escape hatch (it is what
  `testkit.RegisterFakeTransform` uses).

### The SCALAR ROOT asymmetry for transforms

`RegisterTransformT[T, C]` allows `C` to be a **scalar**, not only a struct:
transform plugin blocks are not forced to mappings. The one deliberate
asymmetry exists because the `script` plugin's config *is* the Starlark
source text itself (`script: |` — the block value is a string). A null
`script:` is therefore a type error against the string schema, not an empty
config (`registry.NewTransform` does not normalize nil to an empty object for
transforms). The factory also receives `dir` — the pipeline file's directory
— so configs carrying file paths (wasm `module:`) resolve against it.

## Lifecycle contracts per kind

### Source

```go
type Source interface {
    Init(state []byte) error
    Run(ctx context.Context, emit func(Message))
    Commit(ctx context.Context, throughSrcSeq int64) (state []byte, err error)
    Close() error
}
```

- `Init` receives the persisted state the source previously returned from
  `Commit` — the engine calls it only when state exists. Restore your offset
  here.
- `Run` emits continuously; `emit` blocks under backpressure (the admission
  gate), so no buffering is needed. Set `Message.SrcSeq` to a per-source
  monotonic sequence — it advances the commit watermark.
- `Commit(ctx, throughSrcSeq)` is called whenever the contiguous committed
  frontier advances; commit your offsets *here* (Kafka offsets, file
  offsets, SQL watermarks). The builtin sources use a watermark-bounded
  scan: the scan starts at the last folded watermark and deletes every
  pending entry it visits, keeping each call O(new work) — see
  `kafkaSource.Commit` / `fileSource.Commit` in
  `internal/registry/builtin/`. `Cursor` on emitted messages is what job
  pipelines persist as the sql watermark.
- `PullSource` adds `Pull(ctx, emit) error` for job pipelines: emit rows
  synchronously, return nil on exhaustion (the run commits) or an error
  (the run fails — distinct from per-message dead letters). Sources
  declaring the `"pull"` capability must implement it.

### Sink

```go
type Sink interface {
    Write(ctx context.Context, msgs []Message) error
    Close() error
}
```

Batching is engine-owned (§6.4): the sink receives one batch, writes it,
returns nil or an error; the engine handles retries, optional drops and
dead letters. Success = commit of every message in the batch. There is no
`Init` on sinks — out-of-process sink adapters self-init with their config
for that reason (`docs/plugins.md`).

### Codec

`Decode(raw []byte) (any, error)` and `Encode(v any) ([]byte, error)`.
Codecs referenced by bare name (`decoder: json`) instantiate config-less;
named `codecs:` declarations validate and instantiate at verify with the
pipeline directory for relative paths. Declaration names and registered
codec names are disjoint namespaces (`cfg_codec_shadow` otherwise).

### Transform

```go
type Transform interface {
    Init(env *TransformEnv) error
    Apply(msg *Message) ([]*Message, error)
    Close() error
}
```

- `Init(TransformEnv)` runs once before workers start: the pipeline's frozen
  `Constants` and the run's `Parameters`, a node-scoped `Logf`, and
  `SlowCallWarn` — the advisory slow-call threshold for plugins hosting
  heavyweight engines (the wasm watchdog reads it).
- `Apply` turns one message into **zero or more** messages:
  - zero outputs = the message is committed as filtered and counted
    (`eventboat_fanout_no_match_total`) — the same semantics as an edge
    predicate with no matching edge;
  - one or more outputs fan out downstream; the engine expands the commit
    accounting for the extra branches (the split contract);
  - a non-nil error retries per the incoming edge's delivery policy, then
    dead letters — it never fails the node. Mutate `msg` in place (or return
    copies); the engine fans your outputs out with predicates.
- **`TransformCloner`** — implement it when your execution state is **not
  goroutine-safe** (wasm module instances die on traps). The engine clones
  once per worker goroutine and clones must be independent. If `Clone`
  fails, the engine treats it as node-fatal: `failNode` cancels the
  pipeline and `Run` returns the error — the master instance is exactly what
  the plugin declared unsafe to share, so it must not degrade into being
  shared across workers (`internal/engine/nodes.go`, `runTransform`).
- **`TransformFlavor`** — return `"script"` or `"wasm"` to feed the
  per-flavor duration histograms and budget/timeout counters; anything else
  records generically.
- Declare the `"explain-safe"` capability (script, split) if `explain` may
  dry-run your transform on scratch messages; wasm deliberately does not
  (explain never executes guest code).

## Error classification across plugin boundaries

A plugin cannot raise diagnostics itself, so failures carry classification
in `registry.TransformError`:

| Field | Meaning |
|---|---|
| `Err` | the underlying error |
| `DiagCode` | at **verify** time, routes a factory failure to a specific diagnostic code (empty falls back to `plugin_schema`) — `expr_starlark_compile` and `expr_wasm_compile` survive this way, with a `Hint` |
| `Backtrace` | at **run** time, stored verbatim in the dead letter (Starlark backtraces; positions render as `script:L:C`) |
| `Flag` | marks budget exhaustion (`"steps"`) or timeout (`"timeout"`) — feeds the budget/timeout counters |
| `Flavor` | per-flavor metrics |

`internal/ir.addFactoryDiags` unwraps `TransformError` at verify; the engine
unwraps `Flag`/`Backtrace` at run time (`internal/engine/nodes.go`).

## Writing a custom in-process transform

For tests and third-party compile-in transforms, `internal/testkit` provides
the fastest path:

```go
package mypipeline_test

import (
    "testing"

    "github.com/eventboat/eventboat/internal/registry"
    "github.com/eventboat/eventboat/internal/testkit"
)

func TestUppercaseTransform(t *testing.T) {
    reg := registry.New()
    // RegisterFakeTransform uses the string-schema API with an
    // accept-anything schema, so the fake works with any plugin block.
    err := testkit.RegisterFakeTransform(reg, "upper", testkit.TransformFunc(
        func(msg *registry.Message) ([]*registry.Message, error) {
            if s, ok := msg.Decoded.(map[string]any); ok {
                s["shout"] = "hi"
            }
            return []*registry.Message{msg}, nil
        }))
    if err != nil {
        t.Fatal(err)
    }
    // ... build a pipeline YAML whose transform node uses `upper: {}`
    // and run it through testrun.RunFile or the engine directly.
}
```

`TransformFunc` adapts a plain function to `registry.Transform` (identity
Init/Close); returning a nil slice filters, an error dead-letters after the
edge's delivery retries — both engine contracts, exercised by tests. For the
full out-of-process story — gRPC sources/sinks in any language, the JSON
handshake, manifests, restart policies — see `docs/plugins.md`; note that
**out-of-process gRPC transforms are future work** (transforms are
registered plugins, but compiled-in only).

## Where the builtins live

`internal/registry/builtin/register.go` wires everything:
`source-file`/`cron`/`http_server`/`kafka`/`sql`,
`transform-script`/`split`/`wasm`, `sink-file`/`http`/`kafka`/`drop`,
`codec-json`/`raw`/`csv`/`avro`/`protobuf`. Each registration is a ~30-line
typed function — the best templates for writing your own.
