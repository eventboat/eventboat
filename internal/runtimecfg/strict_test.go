package runtimecfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Candidate 06 acceptance 6b: the typed decode diagnoses BOTH unknown keys
// and type errors — data_dir: 123, enable: "yes" and sample_ratio: -1 used to
// silently fall through to defaults.
func TestStrictTypedDecode(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string // substring of the error
	}{
		{"unknown top-level key", "kind: Runtime\nstorag:\n  data_dir: x\n", "storag"},
		{"unknown storage key", "kind: Runtime\nstorage:\n  spool_retentionn: 1\n", "spool_retentionn"},
		{"data_dir type", "kind: Runtime\nstorage:\n  data_dir: 123\n", "data_dir"},
		{"ephemeral type", "kind: Runtime\nstorage:\n  ephemeral: \"yes\"\n", "ephemeral"},
		{"spool_retention type", "kind: Runtime\nstorage:\n  spool_retention: many\n", "spool_retention"},
		{"write_batch max_rows type", "kind: Runtime\nstorage:\n  write_batch:\n    max_rows: \"256\"\n", "cannot unmarshal"},
		{"write_batch max_wait_ms type", "kind: Runtime\nstorage:\n  write_batch:\n    max_wait_ms: soon\n", "cannot unmarshal"},
		{"write_batch unknown key", "kind: Runtime\nstorage:\n  write_batch:\n    max_batch: 1\n", "max_batch"},
		{"admin enable type", "kind: Runtime\nadmin:\n  enable: \"yes\"\n", "enable"},
		{"admin listen type", "kind: Runtime\nadmin:\n  listen: 7788\n", "listen"},
		{"mcp enable type", "kind: Runtime\nmcp:\n  enable: on\n", "enable"},
		{"sample ratio type", "kind: Runtime\ntelemetry:\n  sample_ratio: often\n", "sample_ratio"},
		{"sample ratio range", "kind: Runtime\ntelemetry:\n  sample_ratio: -1\n", "sample_ratio"},
		{"sample ratio above one", "kind: Runtime\ntelemetry:\n  sample_ratio: 2\n", "sample_ratio"},
		{"prometheus type", "kind: Runtime\ntelemetry:\n  prometheus: \"1\"\n", "prometheus"},
		{"bad apiVersion", "apiVersion: v2\nkind: Runtime\n", "apiVersion"},
		{"bad kind", "kind: Pipeline\n", "kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "eventboat.yaml")
			if err := os.WriteFile(file, []byte(tc.yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := Load(file)
			if err == nil {
				t.Fatalf("accepted invalid value: %s", tc.yaml)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.want)
			}
		})
	}
}

// Absent keys keep their defaults; an explicit empty scalar means "unset".
func TestAbsentKeysKeepDefaults(t *testing.T) {
	file := filepath.Join(t.TempDir(), "eventboat.yaml")
	if err := os.WriteFile(file, []byte("apiVersion: eventboat/v1\nkind: Runtime\nstorage:\n  ephemeral: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.DataDir != "data" || cfg.Storage.SpoolRetention != 10_000 {
		t.Fatalf("storage defaults lost: %+v", cfg.Storage)
	}
	if cfg.Storage.WriteBatch.MaxRows != 256 || cfg.Storage.WriteBatch.MaxWaitMs != 2 {
		t.Fatalf("write_batch defaults lost: %+v", cfg.Storage.WriteBatch)
	}
	if !cfg.Storage.Ephemeral {
		t.Fatal("explicit ephemeral lost")
	}
	if cfg.Admin.Listen != "127.0.0.1:7788" || !cfg.Admin.Enable || !cfg.MCP.Enable {
		t.Fatalf("admin/mcp defaults lost: %+v %+v", cfg.Admin, cfg.MCP)
	}
	if cfg.Telemetry.SampleRatio != 0.1 || !cfg.Telemetry.Prometheus {
		t.Fatalf("telemetry defaults lost: %+v", cfg.Telemetry)
	}
	// sample_ratio: 0 is an explicit "off", not "keep the default".
	if err := os.WriteFile(file, []byte("kind: Runtime\ntelemetry:\n  sample_ratio: 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telemetry.SampleRatio != 0 || !cfg.Telemetry.Prometheus {
		t.Fatalf("explicit sample_ratio 0 not honored: %+v", cfg.Telemetry)
	}
	// mcp.enable: false is honored (candidate 06: the daemon gates /mcp).
	if err := os.WriteFile(file, []byte("kind: Runtime\nmcp:\n  enable: false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MCP.Enable {
		t.Fatal("mcp.enable: false ignored")
	}
}
