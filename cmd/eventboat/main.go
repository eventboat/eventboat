// Command eventboat is the Eventboat v3 POC CLI: verify (static gate), test
// (contract tests), run (execute a pipeline) and the operational verbs,
// dispatched through github.com/lynx-go/commands (dispatch.go assembles the
// verb table; the cmdX functions remain the executors).
package main

import (
	"context"
	"os"

	"github.com/lynx-go/commands"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run dispatches one invocation: the framework owns the help screens, the
// usage-error exit codes and the global --json root bool; the verbs delegate
// to the cmdX executors, which keep their own flag parsing and diagnostics.
func run(args []string) int {
	env := &commands.Environment{Stdout: os.Stdout, Stderr: os.Stderr}
	return newApp().Run(context.Background(), env, args)
}
