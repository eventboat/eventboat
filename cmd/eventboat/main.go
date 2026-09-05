// Command eventboat is the Eventboat CLI. The verb table and executors live
// in internal/cli, shared with the root package's RunCLI — the entry point
// for custom builds that register their own compiled-in plugins (see
// pkg/plugin and examples/custom-build).
package main

import (
	"os"

	"github.com/eventboat/eventboat/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}
