package cli

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/lynx-go/commands"
)

var update = flag.Bool("update", false, "regenerate the golden help snapshots")

// The help surface is framework-rendered (HelpHeader/VerbTitle/HelpFooter,
// printVerbHelp): the format is allowed to change with the dispatch
// migration, but once shipped it is pinned against drift by golden files.
// Regenerate with: go test ./cmd/eventboat -run TestHelpSnapshots -update
func TestHelpSnapshots(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"bare", nil}, // no arguments: help screen, exit 0
		{"root", []string{"--help"}},
		{"help", []string{"help"}},
		{"verify", []string{"help", "verify"}},
		{"test", []string{"help", "test"}},
		{"run", []string{"help", "run"}},
		{"trigger", []string{"help", "trigger"}},
		{"jobs", []string{"help", "jobs"}},
		{"explain", []string{"help", "explain"}},
		{"replay", []string{"help", "replay"}},
		{"repl", []string{"help", "repl"}},
		{"lsp", []string{"help", "lsp"}},
		{"plugin", []string{"help", "plugin"}},
		{"mcp", []string{"help", "mcp"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			env := &commands.Environment{Stdout: &stdout, Stderr: &stderr}
			code := NewApp().Run(context.Background(), env, tc.args)
			if code != commands.ExitOK {
				t.Fatalf("help exit = %d, stderr:\n%s", code, stderr.String())
			}
			golden := filepath.Join("testdata", "help", tc.name+".txt")
			if *update {
				if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, stdout.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("golden snapshot missing (regenerate with -update): %v", err)
			}
			if got := stdout.String(); got != string(want) {
				t.Errorf("help output drifted (-golden +got):\n-%s\n+%s", want, got)
			}
		})
	}
}
