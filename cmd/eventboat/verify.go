package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
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

	lr := config.LoadFile(*configPath)
	diags := append([]config.Diagnostic{}, lr.Diagnostics...)
	if !lr.HasErrors() {
		_, buildDiags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
		diags = append(diags, buildDiags...)
	}

	ok := true
	for i := range diags {
		if diags[i].Severity == "error" || (*strict && diags[i].Severity == "warning") {
			ok = false
		}
	}

	if jsonOut {
		out, _ := json.MarshalIndent(verifyOutput{OK: ok, File: *configPath, Diagnostics: diags}, "", "  ")
		fmt.Println(string(out))
		return exitCode(ok)
	}

	errors, warnings := 0, 0
	for _, d := range diags {
		fmt.Println(d.Error())
		if d.Hint != "" {
			fmt.Printf("    hint: %s\n", d.Hint)
		}
		switch d.Severity {
		case "error":
			errors++
		case "warning":
			warnings++
		}
	}
	fmt.Printf("%s: %d error(s), %d warning(s)\n", *configPath, errors, warnings)
	if !ok {
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
