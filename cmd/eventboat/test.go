package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/eventboat/eventboat/internal/testrun"
)

type testCaseJSON struct {
	Name     string   `json:"name"`
	Status   string   `json:"status"`
	Failures []string `json:"failures,omitempty"`
}

type testOutputJSON struct {
	Suite string         `json:"suite"`
	File  string         `json:"file"`
	OK    bool           `json:"ok"`
	Cases []testCaseJSON `json:"cases"`
}

type testCommandOutputJSON struct {
	Skipped int              `json:"skipped"`
	Suites  []testOutputJSON `json:"suites"`
}

// cmdTest runs contract suites. Directory arguments are walked recursively;
// a YAML file counts as a suite only when it declares a top-level `suite:`
// key — pipelines and unrelated YAML are skipped and counted (output goes to
// out; hard errors go to stderr).
func cmdTest(args []string, jsonOut bool, out io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "test: give test file(s) or directory(ies)")
		return 2
	}
	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "test: builtin registration: %v\n", err)
		return 2
	}

	var files []string
	skipped := 0
	for _, arg := range args {
		info, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "test: %v\n", err)
			return 2
		}
		if !info.IsDir() {
			files = append(files, arg) // explicit file args are always suites
			continue
		}
		err = filepath.WalkDir(arg, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			name := d.Name()
			if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
				return nil
			}
			if !testrun.IsSuite(path) {
				skipped++
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "test: %v\n", err)
			return 2
		}
	}
	if len(files) == 0 {
		if jsonOut {
			b, _ := json.MarshalIndent(testCommandOutputJSON{Skipped: skipped}, "", "  ")
			fmt.Fprintln(out, string(b))
		} else {
			fmt.Fprintln(os.Stderr, "test: no test files found")
		}
		return 2
	}

	allOK := true
	var outputs []testOutputJSON
	for _, file := range files {
		report, err := testrun.RunFile(file, reg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "test: %s: %v\n", file, err)
			allOK = false
			continue
		}
		outEntry := testOutputJSON{Suite: report.Suite, File: file, OK: report.OK()}
		if !report.OK() {
			allOK = false
		}
		if jsonOut {
			for _, c := range report.Cases {
				outEntry.Cases = append(outEntry.Cases, testCaseJSON{Name: c.Name, Status: c.Status, Failures: c.Failures})
			}
			outputs = append(outputs, outEntry)
			continue
		}
		fmt.Fprintf(out, "suite %s (%s)\n", report.Suite, file)
		for _, c := range report.Cases {
			mark := "PASS"
			if c.Status != "pass" {
				mark = "FAIL"
			}
			fmt.Fprintf(out, "  %s  %s\n", mark, c.Name)
			for _, f := range c.Failures {
				fmt.Fprintf(out, "        %s\n", f)
			}
		}
		status := "ok"
		if !report.OK() {
			status = "FAILED"
		}
		fmt.Fprintf(out, "  suite %s\n", status)
	}
	if jsonOut {
		b, _ := json.MarshalIndent(testCommandOutputJSON{Skipped: skipped, Suites: outputs}, "", "  ")
		fmt.Fprintln(out, string(b))
	} else if skipped > 0 {
		fmt.Fprintf(out, "skipped %d non-suite yaml file(s)\n", skipped)
	}
	if !allOK {
		return 1
	}
	return 0
}
