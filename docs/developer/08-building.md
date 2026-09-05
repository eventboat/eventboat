---
title: "Building & release"
order: 8
---

# Building & release

Eventboat builds with the standard Go toolchain and no code generation step
in the normal path: `go build ./...` is the whole story for contributors.
This page covers the local build, the container image, the CI workflows,
and how releases are cut.

## Prerequisites

- **Go 1.25** (matches `go.mod`; the CI jobs pin the same version).
- **Docker** — only for the env-gated integration suites (the Kafka
  testcontainers test) and for building/running the container image.
- Nothing else: all drivers are pure Go (SQLite via `modernc.org/sqlite`,
  Postgres via pgx, Kafka via kafka-go), so CGO is never required.

## Local build and run

```bash
go build -o eventboat ./cmd/eventboat     # the single binary
./eventboat                               # bare invocation prints help, exit 0
./eventboat verify --config examples/linear/pipeline.yaml
./eventboat test examples
./eventboat run --config examples/linear/pipeline.yaml
./eventboat run --config my.yaml --ephemeral   # in-memory store, no ./data
```

The example job pipeline's sqlite source database regenerates with
`go run ./examples/job-sync/seed`. The WASM test guest is committed as a
built artifact (`internal/wasmhost/testdata/aggregate.wasm`), so plain
`go test ./...` needs no wasm toolchain; rebuild it with
`GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o ../aggregate.wasm .`
from `internal/wasmhost/testdata/guest` (CI does exactly this).

## The container image

The `Dockerfile` is two stages:

1. **Build**: `golang:1.25-alpine`, `CGO_ENABLED=0 go build -trimpath` —
   the pure-Go dependency set is what makes the static build possible.
2. **Runtime**: `gcr.io/distroless/static-debian12:nonroot`, binary at
   `/usr/local/bin/eventboat`, running as `nonroot` (uid 65532).

`/pipelines` and `/data` are the conventional mount points (config dir and
SQLite volume — `examples/k8s/deployment.yaml` mounts exactly these; `.keep`
placeholders carry the nonroot-owned directories through COPY and keep
unmounted smoke runs working). No CMD: a bare `docker run` prints the help
screen and exits 0.

```bash
docker build -t eventboat:dev .
docker run --rm -v "$PWD/examples/linear:/work" -w /work eventboat:dev \
  run --config /work/pipeline.yaml
```

## Development data layout

Where `run` puts things on disk (all under `--data-dir`, default `data`;
`--ephemeral` replaces the SQLite store with an in-memory one):

| Path | Written by | Contents |
|---|---|---|
| `data/stores/pipeline.db` | `ops.New`'s default store factory | the shared SQLite store: spool, checkpoint, source states, dead letters, job history |
| `data/pipelines/<name>.yaml` | `ops.Deploy` | the deployed config — persisted verbatim, reloaded on restart and per job run |

The per-pipeline name is the store key namespace, which is why
`metadata.name` is validated so strictly (`cfg_name_invalid`).

## Regenerating protocol code

`pkg/pluginproto` is generated from `proto/eventboat/plugin/v1/plugin.proto`.
Regenerate it only with the pinned protoc-gen-go toolchain the existing
stubs were built with (see the git history for the toolchain version), and
expect a matching ABI bump ripple: external plugins import the generated
stubs, so a regeneration that changes the surface means external plugins
must be rebuilt — record that in the CHANGELOG entry.

## GHCR publishing

`.github/workflows/docker.yml` builds linux/amd64 + linux/arm64 (QEMU +
buildx; CGO off makes cross-builds plain Go cross-compilation) and pushes
`ghcr.io/eventboat/eventboat`:

| Trigger | Tags |
|---|---|
| push to `main` | `:main` and `:sha-<short>` |
| tag `v1.2.3` | `:1.2.3` (semver `{{version}}`) |
| pull request | build validation only, nothing pushed |

There is deliberately **no moving `0.x` tag** while pre-1.0: a floating
`0.2` must not silently point at an rc of a later patch line.

## CI workflows

`.github/workflows/ci.yml` runs on every push to `main` and every PR:

- **test** — build, vet, rebuild the WASM guest from source, then
  `go test -race ./...` (includes the seven invariants, the examples
  verify/test gate, the gRPC plugin acceptance, the CESQL TCK, the wasm
  tests; the env-gated integration packages skip without their env).
- **lint** — golangci-lint v2 (v2.13.2) with the repo config
  (`.golangci.yml`): standard set + misspell + gofmt; exclusions are
  justified in the config.
- **kafka-integration** — the real-broker suite
  (`EVENTBOAT_KAFKA_TEST=1`, testcontainers).
- **bench** — `scripts/bench-gate.sh` (the loose threshold gate) plus the
  informational Starlark-vs-WASM benchmark.

`.github/workflows/soak.yml` — nightly (02:30 UTC) plus manual dispatch:
the soak suite under mixed load with injected faults, duration controlled by
the workflow input (default 25m).

The docs site is published by a GitHub Pages workflow (`.github/workflows/pages.yml`):
it builds and deploys the site from `docs/` changes — content pages and the
generator — so docs edits go live without a release. The rendered result
lives at `eventboat.dev/eventboat/` (the project-pages subpath under the
organization domain).

The public docs site (`eventboat.dev`, the `eventboat/eventboat.github.io`
Hugo repo) consumes this repo's developer guides at ITS build time: its
Pages workflow checks out `eventboat/eventboat` and runs
`tools/sitegen -hugo-out` to render `docs/developer/*.md` into Hugo content
under `content/docs/developer/`, then builds Hugo as usual — plus a daily
scheduled rebuild to pick up upstream doc changes. Nothing is copied into
that repo; this repo stays the single source of truth, so a guide edit here
is live there after the next org-site build (push-triggered or daily).

## The editor story

The LSP (`eventboat lsp`) works with any editor; a minimal VS Code launcher
lives in `examples/editors/vscode` — it spawns the binary over stdio and
wires diagnostics, completion and hover through. There is no marketplace
extension; the example is the integration.

## Release process

- **CHANGELOG discipline**: every user-visible change lands under
  `## Unreleased` in `CHANGELOG.md` at the time of the commit (Keep a
  Changelog format; the repo's entries carry the rationale, not just the
  diff summary). Look at the git log — commits like the Settled→Commit
  rename ship with their full old→new mapping in the entry.
- **Cutting a release**: consolidate `Unreleased` into a dated version
  section, tag `v<semver>`, push the tag. The tag triggers docker.yml, which
  publishes `:<version>` to GHCR. Pre-releases use rc suffixes following
  semver (`v0.1.0-beta`, `v0.2.0-rc1` in the tag history); dependency bumps
  or contract changes that would break plugins ride an rc first (the
  v0.2.0-rc1 release bumped lynx-go/commands for a contract fix).
- **The binary is the release artifact** for non-container users:
  `CGO_ENABLED=0 go build -trimpath -o eventboat ./cmd/eventboat` on any
  platform; no platform-specific code paths exist.

## tools/sitegen — the docs site generator

`tools/sitegen` (in progress) is the documentation site generator: a
**separate Go module** under `tools/`, so the main module's dependency tree
stays untouched. Run it with `go run ./tools/sitegen` from within `tools/`.
It renders the Markdown under `docs/` into a static site with **no CDN
dependencies** — no external JS or fonts — because the site must render
anywhere, including air-gapped environments. Frontmatter (`title`, `order`)
drives page titles and ordering; relative links within `docs/` resolve as
site links; code blocks are plain ASCII-safe fences.
