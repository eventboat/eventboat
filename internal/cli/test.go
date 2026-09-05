package cli

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

type testParseErrorJSON struct {
	File  string `json:"file"`
	Error string `json:"error"`
}

type testCommandOutputJSON struct {
	Skipped     int                  `json:"skipped"`
	ParseErrors []testParseErrorJSON `json:"parse_errors,omitempty"`
	Suites      []testOutputJSON     `json:"suites"`
}

// cmdTest runs contract suites. Directory arguments are walked recursively;
// a YAML file counts as a suite only when it declares a top-level `suite:`
// key — pipelines and unrelated YAML are skipped and counted. YAML that
// cannot be parsed is a hard error: the run reports it (human output and the
// --json parse_errors field), still runs the valid suites, and exits
// non-zero (round-2 review #2). Output goes to out; walk errors go to stderr.
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
	var parseErrors []testParseErrorJSON
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
			isSuite, parseErr := testrun.IsSuite(path)
			if parseErr != nil {
				parseErrors = append(parseErrors, testParseErrorJSON{File: path, Error: parseErr.Error()})
				return nil
			}
			if !isSuite {
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
	if len(parseErrors) > 0 && !jsonOut {
		for _, pe := range parseErrors {
			_, _ = fmt.Fprintf(out, "parse error: %s: %s\n", pe.File, pe.Error)
		}
	}
	if len(files) == 0 {
		if jsonOut {
			b, _ := json.MarshalIndent(testCommandOutputJSON{Skipped: skipped, ParseErrors: parseErrors}, "", "  ")
			_, _ = fmt.Fprintln(out, string(b))
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
		_, _ = fmt.Fprintf(out, "suite %s (%s)\n", report.Suite, file)
		for _, c := range report.Cases {
			mark := "PASS"
			if c.Status != "pass" {
				mark = "FAIL"
			}
			_, _ = fmt.Fprintf(out, "  %s  %s\n", mark, c.Name)
			for _, f := range c.Failures {
				_, _ = fmt.Fprintf(out, "        %s\n", f)
			}
		}
		status := "ok"
		if !report.OK() {
			status = "FAILED"
		}
		_, _ = fmt.Fprintf(out, "  suite %s\n", status)
	}
	if jsonOut {
		b, _ := json.MarshalIndent(testCommandOutputJSON{Skipped: skipped, ParseErrors: parseErrors, Suites: outputs}, "", "  ")
		_, _ = fmt.Fprintln(out, string(b))
	} else if skipped > 0 {
		_, _ = fmt.Fprintf(out, "skipped %d non-suite yaml file(s)\n", skipped)
	}
	if !allOK || len(parseErrors) > 0 {
		return 1
	}
	return 0
}
