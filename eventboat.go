package eventboat

import "github.com/eventboat/eventboat/internal/cli"

// RunCLI dispatches one CLI invocation and returns the process exit code.
// args are the arguments after the program name (typically os.Args[1:]).
// Diagnostics go to stdout/stderr; RunCLI never exits the process — the
// caller owns os.Exit.
func RunCLI(args []string) int {
	return cli.Run(args)
}
