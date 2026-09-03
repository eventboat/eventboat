package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/testrun"
)

// The acceptance gate for examples: every pipeline verifies clean, and every
// suite in examples/*/tests passes against the real engine.
func TestExamplesVerifyAndTest(t *testing.T) {
	examplesDir := "../../examples"
	entries, err := os.ReadDir(examplesDir)
	if err != nil {
		t.Fatal(err)
	}
	pipelines := 0
	suites := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(examplesDir, e.Name())
		pipelinePath := filepath.Join(dir, "pipeline.yaml")
		if _, err := os.Stat(pipelinePath); err != nil {
			continue
		}
		pipelines++

		reg := registry.New()
		if err := builtin.RegisterAll(reg); err != nil {
			t.Fatal(err)
		}
		lr := config.LoadFile(pipelinePath)
		if lr.HasErrors() {
			t.Errorf("%s: verify errors:\n%s", pipelinePath, diagsString(lr.Diagnostics))
			continue
		}
		_, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
		for _, d := range diags {
			if d.Severity == "error" {
				t.Errorf("%s: verify errors:\n%s", pipelinePath, diagsString(diags))
				break
			}
		}

		testsDir := filepath.Join(dir, "tests")
		if info, err := os.Stat(testsDir); err == nil && info.IsDir() {
			testFiles, err := filepath.Glob(filepath.Join(testsDir, "*.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			for _, tf := range testFiles {
				suites++
				report, err := testrun.RunFile(tf, reg)
				if err != nil {
					t.Errorf("%s: %v", tf, err)
					continue
				}
				for _, c := range report.Cases {
					if c.Status != "pass" {
						t.Errorf("%s: case %q failed: %v", tf, c.Name, c.Failures)
					}
				}
			}
		}
	}
	if pipelines < 3 {
		t.Errorf("expected at least 3 example pipelines, found %d", pipelines)
	}
	if suites < 2 {
		t.Errorf("expected at least 2 contract suites, found %d", suites)
	}
}

func diagsString(diags []config.Diagnostic) string {
	out := ""
	for _, d := range diags {
		out += d.Error() + "\n"
		if d.Hint != "" {
			out += "    hint: " + d.Hint + "\n"
		}
	}
	return out
}
