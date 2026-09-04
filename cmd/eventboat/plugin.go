package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// cmdPlugin serves the plugin ABI surface (redesign-v3.md §6.5): the catalog
// lists registered plugins with their ABI versions so configs can pin
// `version:` against a known binary.
func cmdPlugin(args []string, jsonOut bool) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, "usage: eventboat [--json] plugin catalog")
		return 2
	}
	switch args[0] {
	case "catalog":
		return pluginCatalog(jsonOut)
	default:
		fmt.Fprintf(os.Stderr, "unknown plugin subcommand %q\n\nusage: eventboat [--json] plugin catalog", args[0])
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
	fmt.Printf("codecs: %v\n", c.Codecs)
	return 0
}
