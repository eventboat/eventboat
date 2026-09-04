# Eventboat for VS Code (minimal extension)

Diagnostics, completion and hover for Eventboat pipeline YAML — all provided
by the `eventboat lsp` language server built into the binary. This extension
is just the launcher; it is **not published to the marketplace** (POC scope,
redesign-v3.md §7.4 M4).

## One-step setup

```bash
# 1. build the binary and put it on PATH (or note its absolute path)
go build -o eventboat ./cmd/eventboat           # repo root
# e.g. Windows PowerShell:  $env:Path += ";D:\path\to\repo"

# 2. install the extension's one dependency and open VS Code here
cd examples/editors/vscode
npm install
code --extensionDevelopmentPath=.
```

Open any pipeline YAML (e.g. `examples/branching/pipeline.yaml`):

- **Diagnostics** — every edit runs the real verify pipeline (plugin
  schemas, topology, CEL + Starlark compilation); errors appear inline with
  the engine's codes and hints.
- **Completion** — `Ctrl+Space` offers top-level sections, node framework
  fields per section (`decoder`, `from`, `script`, ...), registered plugin
  names, plugin fields from their JSON Schemas, edge attributes under
  `from:` mappings, and codec names after `decoder:`/`encoder:`.
- **Hover** — plugin names show their full field summary (types, defaults,
  descriptions); framework fields show their semantics.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `eventboat.path` | `eventboat` | binary path (absolute if not on PATH) |
| `eventboat.enable` | `true` | start the server for YAML files |

## Notes

- The server treats every YAML document independently (one pipeline per
  file; overlay composition is a verify-CLI concern, not an editor one).
- Completion context is line/indent-based; flow-style mappings
  (`from: {a: {when: ...}}` on one line) validate but complete less richly
  than block style.
