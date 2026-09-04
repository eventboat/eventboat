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
  eventboat [--json] trigger --config <job.yaml> [--parameters '{"from":"..."}']
  eventboat [--json] jobs list --config <job.yaml> [--limit N]
  eventboat [--json] jobs show <run-id> --config <job.yaml>
  eventboat [--json] explain --config <pipeline.yaml> [--message f.json] [--topology]
  eventboat [--json] replay --config <pipeline.yaml> (--dlq | --spool --from N | --job <run-id>) [--dry-run]

Commands:
  verify    statically validate a pipeline: schema, topology, CEL+Starlark
            compilation, job config and semantic lint (redesign-v3.md §3.1)
  test      run contract test suites against the real in-process engine (§3.2)
  run       execute a pipeline (spool+settle+checkpoint; SQLite store);
            job pipelines run under the jobs manager (§5.8)
  trigger   manually fire a job pipeline once, optionally with parameters
            (backfill); prints the run summary
  jobs      job run history: list or show (counts, parameters, dead letters)
  explain   deterministic walkthrough: symbolic, or message-level with real
            CEL evaluation and Starlark dry-run; --topology renders the DAG
  replay    re-inject dead letters (--dlq), a spool window (--spool) or one
            job run's dead letters (--job) into a live pipeline (§3.3)

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
	case "help", "--help", "-h":
		fmt.Print(usageText)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", rest[0], usageText)
		return 2
	}
}
