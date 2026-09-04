package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/eventboat/eventboat/internal/registry"
)

// cmdPlugin serves the plugin ABI surface (redesign-v3.md §6.5): the catalog
// lists registered plugins with their ABI versions so configs can pin
// `version:` against a known binary; `schema` exports the JSON Schemas for
// offline consumers (IDEs, the LSP, agents — M4 §7.4).
func cmdPlugin(args []string, jsonOut bool) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, "usage: eventboat [--json] plugin catalog\n       eventboat [--json] plugin schema <name>\n       eventboat plugin schema --all --dir schemas/")
		return 2
	}
	switch args[0] {
	case "catalog":
		return pluginCatalog(jsonOut)
	case "schema":
		return pluginSchema(args[1:], jsonOut)
	default:
		fmt.Fprintf(os.Stderr, "unknown plugin subcommand %q\n\nusage: eventboat [--json] plugin catalog\n       eventboat [--json] plugin schema <name>\n       eventboat plugin schema --all --dir schemas/", args[0])
		return 2
	}
}

func pluginCatalog(jsonOut bool) int {
	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin: %v\n", err)
		return 1
	}
	c := reg.Catalog()
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(c); err != nil {
			fmt.Fprintf(os.Stderr, "plugin: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Println("sources:")
	for _, s := range c.Sources {
		caps := ""
		if len(s.Capabilities) > 0 {
			caps = fmt.Sprintf("  capabilities: %v", s.Capabilities)
		}
		fmt.Printf("  %s (v%d)%s\n", s.Name, s.Version, caps)
	}
	fmt.Println("sinks:")
	for _, s := range c.Sinks {
		fmt.Printf("  %s (v%d)\n", s.Name, s.Version)
	}
	fmt.Println("codecs:")
	for _, c := range c.Codecs {
		fmt.Printf("  %s (v%d)\n", c.Name, c.Version)
	}
	return 0
}

// schemaEntry is one plugin's exported schema (JSON mode / --all files).
type schemaEntry struct {
	Kind    string `json:"kind"` // sources | sinks | codecs
	Name    string `json:"name"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

// pluginSchema implements `plugin schema <name>` (print one) and
// `plugin schema --all --dir DIR` (write schemas/<kind>/<name>.json).
func pluginSchema(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("plugin schema", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	all := fs.Bool("all", false, "export every registered plugin's schema")
	dir := fs.String("dir", "schemas", "output directory for --all")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	reg, err := commandRegistry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin: %v\n", err)
		return 1
	}

	if *all {
		return exportAllSchemas(reg, *dir)
	}

	names := fs.Args()
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "plugin schema: name a plugin (see plugin catalog) or pass --all")
		return 2
	}
	var entries []schemaEntry
	for _, name := range names {
		found := lookupSchema(reg, name)
		if len(found) == 0 {
			fmt.Fprintf(os.Stderr, "plugin schema: no plugin named %q (see plugin catalog)\n", name)
			return 1
		}
		entries = append(entries, found...)
	}
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(entries); err != nil {
			fmt.Fprintf(os.Stderr, "plugin: %v\n", err)
			return 1
		}
		return 0
	}
	for _, e := range entries {
		pretty, err := prettySchema(e.Schema)
		if err != nil {
			fmt.Fprintf(os.Stderr, "plugin: %v\n", err)
			return 1
		}
		fmt.Printf("# %s %s (ABI v%d)\n%s\n", e.Kind, e.Name, e.Version, pretty)
	}
	return 0
}

// lookupSchema finds a name across all three sections (names are
// section-scoped; a collision lists both entries).
func lookupSchema(reg *registry.Registry, name string) []schemaEntry {
	var out []schemaEntry
	if m, ok := reg.LookupSource(name); ok {
		out = append(out, schemaEntry{Kind: "sources", Name: m.Name, Version: m.Version, Schema: m.Schema})
	}
	if m, ok := reg.LookupSink(name); ok {
		out = append(out, schemaEntry{Kind: "sinks", Name: m.Name, Version: m.Version, Schema: m.Schema})
	}
	if m, ok := reg.LookupCodec(name); ok {
		out = append(out, schemaEntry{Kind: "codecs", Name: m.Name, Version: m.Version, Schema: m.Schema})
	}
	return out
}

func exportAllSchemas(reg *registry.Registry, dir string) int {
	c := reg.Catalog()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "plugin schema: %v\n", err)
		return 1
	}
	count := 0
	write := func(kind, name string, version int, schema string) int {
		pretty, err := prettySchema(schema)
		if err != nil {
			fmt.Fprintf(os.Stderr, "plugin schema: %v\n", err)
			return 1
		}
		kindDir := filepath.Join(dir, kind)
		if err := os.MkdirAll(kindDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "plugin schema: %v\n", err)
			return 1
		}
		path := filepath.Join(kindDir, name+".json")
		if err := os.WriteFile(path, []byte(pretty+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "plugin schema: %v\n", err)
			return 1
		}
		fmt.Printf("%s\n", filepath.ToSlash(path))
		return 0
	}
	for _, s := range c.Sources {
		if write("sources", s.Name, s.Version, s.Schema) != 0 {
			return 1
		}
		count++
	}
	for _, s := range c.Sinks {
		if write("sinks", s.Name, s.Version, s.Schema) != 0 {
			return 1
		}
		count++
	}
	for _, m := range c.Codecs {
		if write("codecs", m.Name, m.Version, m.Schema) != 0 {
			return 1
		}
		count++
	}
	fmt.Fprintf(os.Stderr, "exported %d schemas to %s\n", count, filepath.ToSlash(dir))
	return 0
}

func prettySchema(schema string) (string, error) {
	var doc any
	if err := json.Unmarshal([]byte(schema), &doc); err != nil {
		return "", fmt.Errorf("schema is not valid JSON: %w", err)
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}
