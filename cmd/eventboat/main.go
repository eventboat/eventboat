// Command eventboat is the Eventboat v3 POC CLI: verify (static gate), test
// (contract tests) and run (execute a pipeline).
package main

import (
	"fmt"
	"os"
)

const usageText = `eventboat — agent-native event router (v3 POC)

Usage:
  eventboat [--json] verify --config <pipeline.yaml> [--strict]
  eventboat [--json] test <testfile-or-dir> [...]
  eventboat run --config <pipeline.yaml> [--data-dir DIR] [--ephemeral]
  eventboat run --config-dir <dir> [--runtime runtime.yaml]      # multi-pipeline daemon
  eventboat [--json] trigger --config <job.yaml> [--parameters '{"from":"..."}']
  eventboat [--json] jobs list --config <job.yaml> [--limit N]
  eventboat [--json] jobs show <run-id> --config <job.yaml>
  eventboat [--json] explain --config <pipeline.yaml> [--message f.json] [--topology]
  eventboat [--json] replay --config <pipeline.yaml> (--dlq | --spool --from N | --job <run-id>) [--dry-run]
  eventboat convert <v2-config> [-o out.yaml] [--report report.md]
  eventboat repl [--message sample.json] [--cel 'expr' | --script f.star]
  eventboat lsp                                          # language server over stdio
  eventboat [--json] plugin catalog
  eventboat mcp (--stdio | --http) [--config-dir <dir>] [--data-dir DIR]

Commands:
  verify    statically validate a pipeline: schema, topology, CEL+Starlark
            compilation, job config and semantic lint (redesign-v3.md §3.1)
  test      run contract test suites against the real in-process engine (§3.2)
  run       execute a pipeline (spool+settle+checkpoint; SQLite store);
            job pipelines run under the jobs manager (§5.8); --config-dir
            runs every pipeline in a directory with the admin surface
  trigger   manually fire a job pipeline once, optionally with parameters
            (backfill); prints the run summary
  jobs      job run history: list or show (counts, parameters, dead letters)
  explain   deterministic walkthrough: symbolic, or message-level with real
            CEL evaluation and Starlark dry-run; --topology renders the DAG
  replay    re-inject dead letters (--dlq), a spool window (--spool) or one
            job run's dead letters (--job) into a live pipeline (§3.3)
  convert   translate an archived v2 pipeline (steps/pipeline[]/edges, YAML
            or HOCON) to the v3 three-section form + a migration report
            (§7.3); exits 1 when the output fails verify
  repl      evaluate CEL predicates and Starlark scripts against one sample
            message without running a pipeline (§3.6/§4.4)
  lsp       language server over stdio: verify diagnostics, completion and
            hover for pipeline YAML (editors: examples/editors/vscode)
  plugin    plugin ABI surface: catalog lists registered plugins; schema
            exports JSON Schemas for offline consumers (§6.5)
  mcp       the agent operations surface: MCP tools over stdio (for agent
            hosts) or HTTP (with Admin REST + SSE + read-only UI)

Global flags:
  --json    machine-readable output for agents and CI
`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	jsonOut := false
	var rest []string
	for _, a := range args {
		if a == "--json" {
			jsonOut = true
			continue
		}
		rest = append(rest, a)
	}
	if len(rest) == 0 {
		fmt.Fprint(os.Stderr, usageText)
		return 2
	}
	switch rest[0] {
	case "verify":
		return cmdVerify(rest[1:], jsonOut)
	case "test":
		return cmdTest(rest[1:], jsonOut, os.Stdout)
	case "run":
		return cmdRun(rest[1:], jsonOut)
	case "trigger":
		return cmdTrigger(rest[1:], jsonOut)
	case "jobs":
		return cmdJobs(rest[1:], jsonOut)
	case "explain":
		return cmdExplain(rest[1:], jsonOut)
	case "replay":
		return cmdReplay(rest[1:], jsonOut)
	case "convert":
		return cmdConvert(rest[1:], jsonOut)
	case "repl":
		return cmdRepl(rest[1:], jsonOut)
	case "lsp":
		return cmdLSP(rest[1:], jsonOut)
	case "plugin":
		return cmdPlugin(rest[1:], jsonOut)
	case "mcp":
		return cmdMCP(rest[1:], jsonOut)
	case "help", "--help", "-h":
		fmt.Print(usageText)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", rest[0], usageText)
		return 2
	}
}
