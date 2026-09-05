// Package eventboat is the library entry point for custom Eventboat builds:
// link compiled-in plugins and run the full CLI under your own main
// function, the way Benthos users link service.Register* before service.
// RunCLI.
//
// [RunCLI] dispatches the same verbs as the shipped eventboat binary
// (verify / test / run / trigger / jobs / explain / replay / repl / lsp /
// plugin / mcp) against the process-wide registry, which contains the
// built-in plugins plus everything registered through
// github.com/eventboat/eventboat/pkg/plugin. A custom build is a four-line
// main:
//
//	package main
//
//	import (
//		"os"
//
//		"github.com/eventboat/eventboat"
//		_ "mycorp/eventboat-plugins/kafka" // registers via init()
//	)
//
//	func main() { os.Exit(eventboat.RunCLI(os.Args[1:])) }
//
// The plugin package itself imports only pkg/plugin — never this root
// package, which would drag the whole engine into every importer of the
// plugin (see the pkg/plugin documentation). Runnable reference:
// examples/custom-build.
//
// The other extension route is out-of-process gRPC plugins
// (pkg/pluginproto, docs/plugins.md): any language, process isolation, no
// host recompilation. The two routes share the manifest/schema/version
// semantics and can be combined in one pipeline.
package eventboat
