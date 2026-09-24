package engine

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Candidate 07 acceptance 1: classification is never derived by matching
// error text. This grep gate scans the packages that touch the transform
// failure seam — the hosts, the plugin adapters, the engine and obs — for the
// shapes the stage deleted; tests and testdata are exempt (tests legitimately
// assert message text).
var sniffGates = []struct {
	name string
	re   *regexp.Regexp
}{
	// The exact shapes candidate 07 deleted, kept as named gates so a
	// regression reads as its own failure.
	{"wasm timeout text sniff", regexp.MustCompile(`strings\.Contains\(\s*[A-Za-z_][\w.]*\.Error\(\)\s*,\s*"exceeded"`)},
	{"wasm prefix strip", regexp.MustCompile(`strings\.TrimPrefix\(\s*[A-Za-z_][\w.]*\.Error\(\)\s*,\s*"wasm: "`)},
	{"starlark step text sniff", regexp.MustCompile(`strings\.Contains\(\s*[A-Za-z_][\w.]*\.Msg\s*,\s*"too many steps"`)},
	// The general rule: no host error text may be matched at all in these
	// packages. Type the failure where it is created instead.
	{"host error text matching", regexp.MustCompile(`strings\.(Contains|HasPrefix|HasSuffix|TrimPrefix|TrimSuffix)\([^)]*\.(Error\(\)|Msg)`)},
}

func TestNoFailureTextSniffing(t *testing.T) {
	// Positive control: the gate must recognize the deleted shapes, so a
	// broken pattern cannot pass silently.
	for _, sample := range []string{
		`if strings.Contains(err.Error(), "exceeded") {`,
		`strings.TrimPrefix(err.Error(), "wasm: ")`,
		`if strings.Contains(serr.Msg, "too many steps") {`,
	} {
		matched := false
		for _, gate := range sniffGates {
			if gate.re.MatchString(sample) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("grep gate does not recognize the deleted shape %q", sample)
		}
	}

	dirs := []string{".", "../obs", "../wasmhost", "../lang/starhost", "../registry/builtin"}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, gate := range sniffGates {
				if m := gate.re.Find(src); m != nil {
					t.Errorf("%s: %s: %q", path, gate.name, m)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
