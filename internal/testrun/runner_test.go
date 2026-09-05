package testrun

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
)

// Suite-referenced paths must stay inside the suite ROOT — the parent of the
// suite's directory, matching the documented <root>/tests/<suite>.yaml +
// <root>/pipeline.yaml layout (so `pipeline: ../pipeline.yaml` works) —
// because the suite YAML arrives from agents (MCP test tool), and an
// escaping pipeline or fixture path would point the daemon at arbitrary
// files whose contents failure summaries echo back.
func TestResolveUnder(t *testing.T) {
	base := filepath.Join(t.TempDir(), "tests") // the suite's directory
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	ok := []string{
		"pipeline.yaml",                   // sibling of the suite
		"fixtures/msg.json",               // nested inside
		"../pipeline.yaml",                // the documented convention
		"../shared/fixtures/msg.json",     // elsewhere in the project root
		"./fixtures/../fixtures/msg.json", // cleans inside
		".",
	}
	for _, rel := range ok {
		if _, err := resolveUnder(base, rel); err != nil {
			t.Errorf("resolveUnder(%q): unexpected error %v", rel, err)
		}
	}
	bad := []string{
		"../../escape.yaml",
		"..\\..\\escape.yaml",
		"fixtures/../../../escape.yaml",
		"../..",
	}
	for _, rel := range bad {
		if p, err := resolveUnder(base, rel); err == nil {
			t.Errorf("resolveUnder(%q) = %q, want rejection", rel, p)
		}
	}

	// Absolute-path spellings never escape: filepath.Join appends them as
	// (unopenable) volume-named elements under the suite directory on
	// Windows and as plain subpaths on POSIX — either way the resolved path
	// stays inside the root or the check rejects it outright.
	root := filepath.Dir(base)
	abs := []string{"/etc/passwd"}
	if v := filepath.VolumeName(root); v != "" {
		abs = append(abs, v+string(filepath.Separator)+"elsewhere"+string(filepath.Separator)+"x.yaml")
		other := "C:"
		if v == "C:" {
			other = "D:"
		}
		abs = append(abs, other+string(filepath.Separator)+"x.yaml")
	}
	for _, rel := range abs {
		p, err := resolveUnder(base, rel)
		if err != nil {
			continue // rejected outright — fine
		}
		if under, rerr := filepath.Rel(root, p); rerr != nil || under == ".." || strings.HasPrefix(under, ".."+string(filepath.Separator)) || filepath.IsAbs(under) {
			t.Errorf("resolveUnder(%q) = %q escaped the root %s", rel, p, root)
		}
	}
}

// RunFile refuses a suite whose pipeline reference escapes the suite root,
// naming the offending path (the file is never opened).
func TestRunFileRejectsTraversalPipeline(t *testing.T) {
	dir := t.TempDir()
	suite := filepath.Join(dir, "suite.yaml")
	if err := os.WriteFile(suite, []byte(`
suite: escape
pipeline: ../../stolen.yaml
cases:
  - name: c
    inject: { at: in, raw: '{}' }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := RunFile(suite, registry.New())
	if err == nil {
		t.Fatal("expected traversal rejection")
	}
	if !strings.Contains(err.Error(), "../../stolen.yaml") || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("error should name the offending path and the escape: %v", err)
	}
}
