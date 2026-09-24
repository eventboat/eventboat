package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/verify"
)

// verifyOutput is the --json shape of the verify command.
type verifyOutput struct {
	OK          bool                `json:"ok"`
	File        string              `json:"file"`
	Diagnostics []config.Diagnostic `json:"diagnostics"`
}

func cmdVerify(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	configPath := fs.String("config", "", "pipeline configuration file")
	strict := fs.Bool("strict", false, "upgrade warnings to errors")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "verify: --config is required")
		return 2
	}

	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: builtin registration: %v\n", err)
		return 2
	}

	// One composition (candidate 05): the CLI reports exactly what MCP,
	// Admin and the LSP report for the same content; only the strict policy
	// differs, and it is applied inside the composition.
	res := verify.File(*configPath, reg, verify.Options{Strict: *strict})
	diags := res.Diagnostics

	if jsonOut {
		out, _ := json.MarshalIndent(verifyOutput{OK: res.OK, File: *configPath, Diagnostics: diags}, "", "  ")
		fmt.Println(string(out))
		return exitCode(res.OK)
	}

	errors, warnings := len(diags.Errors()), len(diags.Warnings())
	for _, d := range diags {
		fmt.Println(d.Error())
		if d.Hint != "" {
			fmt.Printf("    hint: %s\n", d.Hint)
		}
	}
	fmt.Printf("%s: %d error(s), %d warning(s)\n", *configPath, errors, warnings)
	if !res.OK {
		if *strict && warnings > 0 {
			fmt.Println("verify failed (--strict: warnings are errors)")
		} else {
			fmt.Println("verify failed")
		}
		return 1
	}
	fmt.Println("verify ok")
	return 0
}

func exitCode(ok bool) int {
	if ok {
		return 0
	}
	return 1
}
