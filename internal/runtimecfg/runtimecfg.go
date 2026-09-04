// Package runtimecfg is the deployment-level configuration (open question
// #10, M2 review R13): storage location, admin listener, MCP toggle and
// telemetry endpoints. Pipeline resources stay in their own files; this is
// the "runtime vs resource" split of redesign-v3.md §5.10.
//
// Resolution order: explicit --runtime file, then ./eventboat.yaml, then
// defaults; CLI flags override file values.
package runtimecfg

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the typed Runtime configuration (kind: Runtime).
type Config struct {
	Storage   Storage
	Admin     Admin
	MCP       MCP
	Telemetry Telemetry
}

type Storage struct {
	DataDir   string
	Ephemeral bool
}

type Admin struct {
	Listen string
	Enable bool
}

type MCP struct {
	Enable bool
}

type Telemetry struct {
	OTLPEndpoint string
	SampleRatio  float64
	Prometheus   bool
}

// Default returns the defaults used when no file exists.
func Default() Config {
	return Config{
		Storage:   Storage{DataDir: "data"},
		Admin:     Admin{Listen: "127.0.0.1:7788", Enable: true},
		MCP:       MCP{Enable: true},
		Telemetry: Telemetry{SampleRatio: 0.1, Prometheus: true},
	}
}

// Load resolves the runtime configuration. path may be empty (then
// ./eventboat.yaml is tried, falling back to defaults). Unknown keys are
// errors — the same strictness as pipeline configs.
func Load(path string) (Config, error) {
	cfg := Default()
	file := path
	if file == "" {
		if _, err := os.Stat("eventboat.yaml"); err == nil {
			file = "eventboat.yaml"
		} else {
			return cfg, nil
		}
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return cfg, fmt.Errorf("runtime config: %w", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return cfg, fmt.Errorf("runtime config %s: %w", file, err)
	}
	if v, ok := doc["apiVersion"].(string); ok && v != "eventboat/v3" {
		return cfg, fmt.Errorf("runtime config %s: apiVersion must be eventboat/v3", file)
	}
	if v, ok := doc["kind"].(string); ok && v != "Runtime" {
		return cfg, fmt.Errorf("runtime config %s: kind must be Runtime", file)
	}
	allowed := map[string]bool{"apiVersion": true, "kind": true, "storage": true, "admin": true, "mcp": true, "telemetry": true}
	for k := range doc {
		if !allowed[k] {
			return cfg, fmt.Errorf("runtime config %s: unknown key %q (allowed: storage, admin, mcp, telemetry)", file, k)
		}
	}
	if m, ok := doc["storage"].(map[string]any); ok {
		for k := range m {
			switch k {
			case "data_dir", "ephemeral":
			default:
				return cfg, fmt.Errorf("runtime config %s: unknown storage key %q", file, k)
			}
		}
		if v, ok := m["data_dir"].(string); ok && v != "" {
			cfg.Storage.DataDir = v
		}
		if v, ok := m["ephemeral"].(bool); ok {
			cfg.Storage.Ephemeral = v
		}
	}
	if m, ok := doc["admin"].(map[string]any); ok {
		for k := range m {
			switch k {
			case "listen", "enable":
			default:
				return cfg, fmt.Errorf("runtime config %s: unknown admin key %q", file, k)
			}
		}
		if v, ok := m["listen"].(string); ok && v != "" {
			cfg.Admin.Listen = v
		}
		if v, ok := m["enable"].(bool); ok {
			cfg.Admin.Enable = v
		}
	}
	if m, ok := doc["mcp"].(map[string]any); ok {
		for k := range m {
			switch k {
			case "enable":
			default:
				return cfg, fmt.Errorf("runtime config %s: unknown mcp key %q", file, k)
			}
		}
		if v, ok := m["enable"].(bool); ok {
			cfg.MCP.Enable = v
		}
	}
	if m, ok := doc["telemetry"].(map[string]any); ok {
		for k := range m {
			switch k {
			case "otlp_endpoint", "sample_ratio", "prometheus":
			default:
				return cfg, fmt.Errorf("runtime config %s: unknown telemetry key %q", file, k)
			}
		}
		if v, ok := m["otlp_endpoint"].(string); ok {
			cfg.Telemetry.OTLPEndpoint = strings.TrimSpace(v)
		}
		if v, ok := m["sample_ratio"].(float64); ok && v > 0 {
			cfg.Telemetry.SampleRatio = v
		}
		if v, ok := m["prometheus"].(bool); ok {
			cfg.Telemetry.Prometheus = v
		}
	}
	return cfg, nil
}
