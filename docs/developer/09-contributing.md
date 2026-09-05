---
title: "Contributing"
order: 9
---

# Contributing

This repo has a strong house style — it is what keeps a codebase with this
many guarantees reviewable. Read this page before your first PR; most of it
is enforceable by CI, but the commit message convention and the comment
voice are reviewed by humans.

## Dev workflow

- Branch from `main`; keep **one logical change per commit** — the git log
  reads as a series of self-contained rationales, and bisectability depends
  on it.
- Run the fast loop before pushing: `go build ./...`, `go test -race ./...`
  (CI runs the race build on every push), and `bash scripts/bench-gate.sh`
  if you touched a hot path.
- User-visible changes get a `CHANGELOG.md` **Unreleased** entry at commit
  time, not at release time. Entries carry a `Changed`/`Added`/`Fixed`/
  `Removed` heading and the same paragraph voice as the commit — the
  Settled→Commit rename entry, for example, includes the complete old→new
  mapping because external consumers needed it to migrate.

## The commit message convention

This repo's commits are **single, detailed, paragraph-led messages written in
the imperative** that explain WHAT and WHY, with concrete file and identifier
references. No one-line subjects, no bullet laundry lists (a short list is
acceptable *inside* the paragraph when enumerating parallel changes). Read a
few from `git log` — e.g. the `Rename Settled → Commit everywhere` commit or
the `Promote transforms to registered plugins` commit — they record the
motivation, the ruling that decided contested points, the migration/compat
story, and the verification done (which tests were run). A good test: could
a reviewer reconstruct the change from the message alone in a year?

Rules of thumb distilled from the log:

- First sentence = the change, imperative ("Add ...", "Fix ...", "Remove
  ...", "Rename ...").
- Then the why, naming the ruling or review finding that decided it (e.g.
  "review R6", "M3-audit J2 ruling") — see
  [Design discussion](#where-design-discussion-lives) below.
- Then the mechanics, with real file paths and identifiers
  (`internal/engine/commit.go`, `RegisterTransformT`, `plugin_schema`).
- End with what was verified ("full suite passes incl. -race on the
  engine").

## Code conventions

- **gofmt and golangci-lint clean.** `.golangci.yml` is deliberately small:
  the standard set (errcheck, govet, ineffassign, staticcheck, unused) plus
  misspell and gofmt — a hygiene floor, not a style machine. Exclusions
  (generated stubs in `pkg/pluginv1`, the vendored CESQL TCK, a quickfix
  class) must be justified in the config; new linters join with a ruling.
- **Comment voice: comments state constraints and rationale, never narrate
  the obvious.** A representative example,
  `internal/registry/builtin/kafka.go`:

  > Commit runs on every frontier advance (~per message), so the scan must
  > start at the watermark, not at 1: the scan itself deletes every seq it
  > visits, which keeps each call O(new work) instead of O(all emissions).

  The comment gives the constraint (called per message), the decision (scan
  from the watermark), and the consequence (O(new work)). "increments the
  counter" comments do not exist here. If a comment only restates the code,
  delete it; if a constraint lives only in a review document, move the
  essential sentence into the code.
- **The full-name principle for YAML vocabulary.** Config keys are full
  words in snake_case — `max_in_flight`, `drain_timeout`,
  `skip_if_successful`, `catchup_window`, `span_sample_rate` — never
  abbreviations. The single recorded exception is `dlq` (retained
  industry-wide, spec v1.6). New keys follow the same rule; if you are
  tempted to abbreviate, the key is probably too long.
- **Error wrapping with `%w`** so classification survives: the engine
  unwraps `registry.TransformError` with `errors.As`, and store/registry
  errors are matched by identity, not string comparison. Wrap, never
  re-format, an error you pass along.

## The "when you touch X you must also Y" checklist

| You touch... | You must also... |
|---|---|
| a plugin config struct (new field) | add `schema` tags; regenerate the goldens (`go test ./internal/registry/builtin -update-schemas`); check whether a runtimecfg/registry test pins the unknown-key behavior |
| a CLI flag or usage string | regenerate help goldens (`go test ./cmd/eventboat -update`) |
| a metric | add the instrument in `internal/obs/obs.go`; record it where the event happens; this developer guide's metrics table |
| a diagnostic code | add it to the diagnostics table in [Configuration & diagnostics](04-config-pipeline.md); codes are API — do not rename, retire with a mapping in the CHANGELOG |
| the engine's delivery/commit paths | keep the seven invariants green and run `-race`; extend the relevant `TestInvariant_*` scenario if you changed a guarantee |
| the spool/checkpoint format or retention | the retention tests (`internal/engine/retention_test.go`) and the kill-9 replay invariant |
| a user-visible behavior | a CHANGELOG Unreleased entry in the same commit |

## Adding a builtin plugin

The checklist for a new compiled-in plugin (source, sink, codec or
transform) — every step has a test that fails until you do it:

1. Define one config struct: `json` tags name the keys, `schema` tags the
   constraints ([Plugin system](03-plugins.md)).
2. Write the register function (typed: `RegisterSourceT`/`RegisterSinkT`/
   `RegisterCodecT`/`RegisterTransformT`) and wire it into `RegisterAll`
   (`internal/registry/builtin/register.go`). Cross-field rules JSON Schema
   cannot express stay in the build function.
3. Regenerate the schema goldens (`go test ./internal/registry/builtin
   -update-schemas`) and review the diff.
4. Catalog surfaces are automatic — `plugin catalog`, `plugin schema`, the
   MCP `catalog` tool and the LSP completion read the registry — but their
   tests may need the new entry.
5. Codecs add a conformance case (`codecs_conformance_test.go`); the
   protobuf codec's descriptor fixture regenerates with `-update-descr`.
6. Remember the name rules: the plugin name must not collide with framework
   fields (registration rejects it) and should follow the full-name
   principle below.

## PR expectations

- Full suite green (`go test ./...`), with `-race` on the engine and store
  packages (CI does this; run it locally first).
- The examples gate stays green: every pipeline under `examples/` must
  verify and pass its contract suites (`cmd/eventboat/examples_test.go`
  runs it in CI).
- Golden diffs (help screens, plugin schemas) present and reviewed line by
  line — a schema golden diff that "just regenerates" without discussion is
  a red flag.
- New public surface (CLI verb, metric, config key, diagnostic code) follows
  the checklist above.

## Where design discussion lives

`redesign-v3.md` is the spec and the single source of truth. Design
discussion has a discipline: contested points are resolved as numbered
**rulings** recorded in the spec's revision log (and echoed in README
decision ledgers), citing the review round that raised them
(`redesign-v3-review*.md`). If your change settles a design question, add
the ruling — reviewers should be able to trace every non-obvious decision
to a dated entry. Dated review documents keep their historical wording even
after later renames (the Settled→Commit rename explicitly left them alone).

## Licensing

Eventboat is Apache-2.0 (`LICENSE`). By contributing you agree your
contributions are licensed under the same terms. Keep the generated protocol
stubs in `pkg/pluginv1` regenerable from
`proto/eventboat/plugin/v1/plugin.proto` — do not hand-edit them.
