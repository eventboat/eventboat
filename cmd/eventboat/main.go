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

Commands:
  verify    statically validate a pipeline: schema, topology, CEL+Starlark
            compilation and semantic lint (redesign-v3.md §3.1)
  test      run contract test suites against the real in-process engine (§3.2)
  run       execute a pipeline (spool+settle+checkpoint; SQLite store)

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
	case "help", "--help", "-h":
		fmt.Print(usageText)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", rest[0], usageText)
		return 2
	}
}
