package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// One-shot modes; the interactive loop shares the same eval helpers.
func TestReplCEL(t *testing.T) {
	sample := filepath.Join(t.TempDir(), "msg.json")
	if err := os.WriteFile(sample, []byte(`{"total": 120, "region": "eu"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdRepl([]string{"--message", sample, "--cel", `payload.total > 100 && payload.region == "eu"`}, false); code != 0 {
		t.Fatalf("cel exit = %d", code)
	}
	// A false predicate is a valid evaluation, not a failure (exit 0).
	if code := cmdRepl([]string{"--message", sample, "--cel", `payload.total > 999`}, false); code != 0 {
		t.Fatalf("false predicate exit = %d, want 0", code)
	}
	if code := cmdRepl([]string{"--cel", "payload.no_such_fn()"}, false); code != 1 {
		t.Fatalf("compile error exit = %d, want 1", code)
	}
}

func TestReplScript(t *testing.T) {
	sample := filepath.Join(t.TempDir(), "msg.json")
	if err := os.WriteFile(sample, []byte(`{"price": 20, "qty": 6}`), 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "total.star")
	if err := os.WriteFile(script, []byte("payload.total = payload.price * payload.qty\nmeta.tier = \"vip\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdRepl([]string{"--message", sample, "--script", script}, false); code != 0 {
		t.Fatalf("script exit = %d", code)
	}
	broken := filepath.Join(t.TempDir(), "broken.star")
	if err := os.WriteFile(broken, []byte("payload.x = undefined_thing\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdRepl([]string{"--message", sample, "--script", broken}, false); code != 1 {
		t.Fatalf("runtime error exit = %d, want 1", code)
	}
}
