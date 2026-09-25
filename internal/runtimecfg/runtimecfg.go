// Package runtimecfg is the deployment-level configuration (open question
// #10, M2 review R13): storage location, admin listener, MCP toggle,
// telemetry endpoints and the store's group-commit write batch. Pipeline
// resources stay in their own files; this is the "runtime vs resource" split
// of redesign-v3.md §5.10.
//
// Resolution order: explicit --runtime file, then ./eventboat.yaml, then
// defaults; CLI flags override file values.
//
// The decode is typed and strict (candidate 06): unknown keys AND type errors
// are diagnosed, so `data_dir: 123`, `enable: "yes"` and `sample_ratio: -1`
// fail loudly instead of silently falling through to defaults.
package runtimecfg

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the typed Runtime configuration (kind: Runtime). APIVersion and
// Kind are accepted document metadata; a mismatching value is an error.
type Config struct {
	APIVersion string    `yaml:"apiVersion"`
	Kind       string    `yaml:"kind"`
	Storage    Storage   `yaml:"storage"`
	Admin      Admin     `yaml:"admin"`
	MCP        MCP       `yaml:"mcp"`
	Telemetry  Telemetry `yaml:"telemetry"`
}

type Storage struct {
	DataDir   string `yaml:"data_dir"`
	Ephemeral bool   `yaml:"ephemeral"`
	// SpoolRetention is how many spool rows stay behind the checkpoint
	// (spool_retention): rows older than that are history and get deleted as
	// the checkpoint advances, bounding SQLite disk and --ephemeral memory on
	// long runs. 0 = engine default (10_000).
	SpoolRetention int64 `yaml:"spool_retention"`
	// WriteBatch is the group-commit tuning surface (§2.6.2).
	WriteBatch WriteBatch `yaml:"write_batch"`
}

// WriteBatch configures the store's single-writer group commit.
type WriteBatch struct {
	// MaxRows caps one multi-row spool INSERT (write_batch.max_rows). Larger
	// batches amortize the transaction; the group is bounded by how many
	// callers are blocked on the store at once, so this is a ceiling, not a
	// trigger. Must be >= 1.
	MaxRows int `yaml:"max_rows"`
	// MaxWaitMs is the ceiling on how long a queued write may wait for
	// companions before its group commits (write_batch.max_wait_ms). 0 =
	// write-through. Must be >= 0.
	MaxWaitMs int `yaml:"max_wait_ms"`
}

type Admin struct {
	Listen string `yaml:"listen"`
	Enable bool   `yaml:"enable"`
	// Token is the bearer token of the admin HTTP surface (empty = none;
	// mandatory for non-loopback binds — see internal/admin.Security).
	Token string `yaml:"token"`
}

type MCP struct {
	Enable bool `yaml:"enable"`
}

type Telemetry struct {
	OTLPEndpoint string  `yaml:"otlp_endpoint"`
	SampleRatio  float64 `yaml:"sample_ratio"`
	Prometheus   bool    `yaml:"prometheus"`
}

// Default returns the defaults used when no file exists and the base values
// an existing file overlays.
func Default() Config {
	return Config{
		APIVersion: "eventboat/v1",
		Kind:       "Runtime",
		Storage: Storage{
			DataDir:        "data",
			SpoolRetention: 10_000,
			WriteBatch:     WriteBatch{MaxRows: 256, MaxWaitMs: 2},
		},
		Admin:     Admin{Listen: "127.0.0.1:7788", Enable: true},
		MCP:       MCP{Enable: true},
		Telemetry: Telemetry{SampleRatio: 0.1, Prometheus: true},
	}
}

// Load resolves the runtime configuration. path may be empty (then
// ./eventboat.yaml is tried, falling back to defaults). Absent keys keep
// their defaults; unknown keys and type errors are errors.
func Load(path string) (Config, error) {
	defaults := Default()
	file := path
	if file == "" {
		if _, err := os.Stat("eventboat.yaml"); err == nil {
			file = "eventboat.yaml"
		} else {
			return defaults, nil
		}
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return defaults, fmt.Errorf("runtime config: %w", err)
	}

	cfg := defaults
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return defaults, fmt.Errorf("runtime config %s: %w", file, err)
	}
	// yaml.v3 coerces scalars into string/bool fields (`data_dir: 123` becomes
	// "123", `enable: "yes"` becomes true); reject those coercions by checking
	// the raw document's value types against the leaf schema.
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return defaults, fmt.Errorf("runtime config %s: %w", file, err)
	}
	if err := checkScalarTypes(file, doc); err != nil {
		return defaults, err
	}

	if cfg.APIVersion != "eventboat/v1" {
		return defaults, fmt.Errorf("runtime config %s: apiVersion must be eventboat/v1", file)
	}
	if cfg.Kind != "Runtime" {
		return defaults, fmt.Errorf("runtime config %s: kind must be Runtime", file)
	}
	// Empty values keep the default (the documented "unset" spelling of a
	// scalar); a wrong TYPE was already rejected by the typed decode.
	if cfg.Storage.DataDir == "" {
		cfg.Storage.DataDir = defaults.Storage.DataDir
	}
	if cfg.Storage.SpoolRetention < 0 {
		return defaults, fmt.Errorf("runtime config %s: storage.spool_retention must be >= 0 (rows kept behind the checkpoint)", file)
	}
	if cfg.Storage.WriteBatch.MaxRows < 1 {
		return defaults, fmt.Errorf("runtime config %s: storage.write_batch.max_rows must be >= 1 (rows per group-commit batch)", file)
	}
	if cfg.Storage.WriteBatch.MaxWaitMs < 0 {
		return defaults, fmt.Errorf("runtime config %s: storage.write_batch.max_wait_ms must be >= 0 (milliseconds; 0 = write-through)", file)
	}
	if cfg.Admin.Listen == "" {
		cfg.Admin.Listen = defaults.Admin.Listen
	}
	cfg.Admin.Token = strings.TrimSpace(cfg.Admin.Token)
	cfg.Telemetry.OTLPEndpoint = strings.TrimSpace(cfg.Telemetry.OTLPEndpoint)
	if cfg.Telemetry.SampleRatio < 0 || cfg.Telemetry.SampleRatio > 1 {
		return defaults, fmt.Errorf("runtime config %s: telemetry.sample_ratio must be a number in [0, 1], got %v", file, cfg.Telemetry.SampleRatio)
	}
	return cfg, nil
}

// scalarRules is the runtime config's leaf schema: dotted path → YAML value
// kind. It exists because yaml.v3's decoder coerces scalar tags into string
// and bool fields; the whole-document type check is what makes
// `data_dir: 123`, `enable: "yes"` and `write_batch.max_rows: "256"` errors
// instead of silent defaults or silent coercions (candidate 06).
var scalarRules = []struct{ path, kind string }{
	{"storage.data_dir", "string"},
	{"storage.ephemeral", "bool"},
	{"storage.spool_retention", "int"},
	{"storage.write_batch.max_rows", "int"},
	{"storage.write_batch.max_wait_ms", "int"},
	{"admin.listen", "string"},
	{"admin.enable", "bool"},
	{"admin.token", "string"},
	{"mcp.enable", "bool"},
	{"telemetry.otlp_endpoint", "string"},
	{"telemetry.sample_ratio", "number"},
	{"telemetry.prometheus", "bool"},
}

func checkScalarTypes(file string, doc map[string]any) error {
	for _, rule := range scalarRules {
		v, ok := lookupPath(doc, rule.path)
		if !ok {
			continue // absent, or a non-mapping already rejected by the typed decode
		}
		if !matchesKind(v, rule.kind) {
			return fmt.Errorf("runtime config %s: %s must be a %s, got %v (%T)", file, rule.path, rule.kind, v, v)
		}
	}
	return nil
}

// lookupPath walks a dotted path through the raw document; a missing key or a
// non-mapping intermediate reports absent.
func lookupPath(doc map[string]any, path string) (any, bool) {
	var cur any = doc
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func matchesKind(v any, kind string) bool {
	switch kind {
	case "string":
		_, ok := v.(string)
		return ok
	case "bool":
		_, ok := v.(bool)
		return ok
	case "int":
		_, ok := v.(int)
		return ok
	case "number":
		// yaml.v3 decodes whole numbers as int and decimals as float64.
		switch v.(type) {
		case int, int64, float64:
			return true
		}
		return false
	}
	return false
}
