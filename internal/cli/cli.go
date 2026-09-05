// Package cli hosts the Eventboat command-line interface: the verb table
// (verify / test / run / trigger / jobs / explain / replay / repl / lsp /
// plugin / mcp, dispatched on github.com/lynx-go/commands), the per-verb
// cmdX executors and their tests.
//
// Two entry points share it: cmd/eventboat (the shipped binary) and the
// root package's RunCLI — the entry point for custom builds that register
// their own compiled-in plugins before running (see pkg/plugin and
// examples/custom-build). Run never exits the process; callers own os.Exit.
package cli

import (
	"context"
	"os"

	"github.com/lynx-go/commands"
)

// Run dispatches one invocation and returns the exit code: the framework
// owns the help screens, the usage-error exit codes and the global --json
// root bool; the verbs delegate to the cmdX executors, which keep their own
// flag parsing and diagnostics.
func Run(args []string) int {
	env := &commands.Environment{Stdout: os.Stdout, Stderr: os.Stderr}
	return NewApp().Run(context.Background(), env, args)
}
