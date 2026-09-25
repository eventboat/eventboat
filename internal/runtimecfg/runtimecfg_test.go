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
apiVersion: eventboat/v1
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

// storage.write_batch (design §2.6.2) decodes strictly: values are validated,
// absent keys keep the 256/2 defaults, and 0 is a legal write-through value,
// not "unset".
func TestWriteBatchConfig(t *testing.T) {
	file := filepath.Join(t.TempDir(), "eventboat.yaml")
	write := func(yaml string) (Storage, error) {
		t.Helper()
		if err := os.WriteFile(file, []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(file)
		return cfg.Storage, err
	}

	st, err := write("apiVersion: eventboat/v1\nkind: Runtime\n")
	if err != nil {
		t.Fatal(err)
	}
	if st.WriteBatch.MaxRows != 256 || st.WriteBatch.MaxWaitMs != 2 {
		t.Fatalf("default write_batch = %+v, want {256 2}", st.WriteBatch)
	}

	st, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_rows: 1000\n    max_wait_ms: 5\n")
	if err != nil {
		t.Fatal(err)
	}
	if st.WriteBatch.MaxRows != 1000 || st.WriteBatch.MaxWaitMs != 5 {
		t.Fatalf("write_batch override = %+v", st.WriteBatch)
	}

	st, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_rows: 64\n    max_wait_ms: 0\n")
	if err != nil {
		t.Fatal(err)
	}
	if st.WriteBatch.MaxRows != 64 || st.WriteBatch.MaxWaitMs != 0 {
		t.Fatalf("explicit write-through = %+v, want {64 0}", st.WriteBatch)
	}

	if _, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_rows: 0\n"); err == nil {
		t.Error("max_rows: 0 accepted (a batch of zero rows cannot commit anything)")
	}
	if _, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_rows: -1\n"); err == nil {
		t.Error("negative max_rows accepted")
	}
	if _, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_wait_ms: -1\n"); err == nil {
		t.Error("negative max_wait_ms accepted")
	}
	if _, err = write("kind: Runtime\nstorage:\n  write_batch:\n    max_rows: 100\n    max_wait_millis: 5\n"); err == nil {
		t.Error("unknown write_batch key accepted")
	}
}
