# custom-build — your own eventboat binary with compiled-in plugins

The compile-time extension route (docs/plugins.md compares it with the
out-of-process gRPC route): link Go plugins into a binary that behaves
exactly like the shipped `eventboat` CLI — same verbs, verify gate, plugin
catalog, LSP and MCP surface, with your plugins first-class in all of them.

## Layout

- `myecho/` — a sink plugin that registers itself via `pkg/plugin` in its
  `init` function. It imports only `pkg/plugin` — never the root package —
  so anything that depends on it stays light; only the final binary links
  the engine.
- `main.go` — the whole binary: blank-import the plugin, delegate to
  `eventboat.RunCLI`.
- `myecho.pipeline.yaml` — the plugin used like any built-in (plugin name
  as the block key; unknown fields are verify errors, same rules). The
  file is deliberately not named `pipeline.yaml`, which the repo's examples
  gate reserves for builtin-only pipelines.

## Try it

    go build -o my-eventboat .
    ./my-eventboat plugin catalog                      # myecho next to the builtins
    ./my-eventboat verify --config myecho.pipeline.yaml
    ./my-eventboat run --config myecho.pipeline.yaml   # a daemon: stop with Ctrl-C

Then delete `output/` and re-run; edit `myecho/myecho.go` and rebuild —
the plugin is ordinary Go code in your module. The repo's root-package
test (`TestCustomBuildAcceptance`) runs this exact chain in CI.
