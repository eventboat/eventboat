package runtimecfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSpoolRetention(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "eventboat.yaml")
	if err := os.WriteFile(file, []byte(`
apiVersion: eventboat/v3
kind: Runtime
storage:
  data_dir: elsewhere
  spool_retention: 2500
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.DataDir != "elsewhere" {
		t.Errorf("data_dir = %q", cfg.Storage.DataDir)
	}
	if cfg.Storage.SpoolRetention != 2500 {
		t.Errorf("spool_retention = %d, want 2500", cfg.Storage.SpoolRetention)
	}

	// Default: 0 means "engine default", never a bogus zero-window trim.
	// (No ./eventboat.yaml in this package's directory, so Load("") falls
	// through to defaults.)
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.SpoolRetention != 10_000 {
		t.Errorf("default spool_retention = %d, want 10000", cfg.Storage.SpoolRetention)
	}

	// Negative windows are config errors, not "trim everything".
	if err := os.WriteFile(file, []byte(`
kind: Runtime
storage:
  spool_retention: -1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Error("negative spool_retention accepted")
	}

	// Unknown storage keys stay errors (strictness unchanged).
	if err := os.WriteFile(file, []byte(`
kind: Runtime
storage:
  spool_retentionn: 1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Error("unknown storage key accepted")
	}
}
