package lsp

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// schemaProp is the completion/hover projection of one JSON Schema property.
type schemaProp struct {
	Type        string
	Description string
	Default     string
	Required    bool
}

// parseSchemaProperties extracts the flat property set from a plugin's JSON
// Schema (all builtin plugin schemas are flat objects — nested plugin
// configuration is not offered structurally). The schema is decoded loosely
// because `type` may be a string or a union array.
func parseSchemaProperties(schemaJSON string) (map[string]schemaProp, error) {
	var loose map[string]json.RawMessage
	if err := json.Unmarshal([]byte(schemaJSON), &loose); err != nil {
		return nil, err
	}
	propsRaw, ok := loose["properties"]
	if !ok {
		return map[string]schemaProp{}, nil
	}
	var props map[string]struct {
		Type        json.RawMessage `json:"type"`
		Description string          `json:"description"`
		Default     json.RawMessage `json:"default"`
	}
	if err := json.Unmarshal(propsRaw, &props); err != nil {
		return nil, err
	}
	var required []string
	if reqRaw, ok := loose["required"]; ok {
		if err := json.Unmarshal(reqRaw, &required); err != nil {
			return nil, err
		}
	}
	reqSet := map[string]bool{}
	for _, r := range required {
		reqSet[r] = true
	}
	out := make(map[string]schemaProp, len(props))
	for name, p := range props {
		typeStr := strings.Trim(strings.TrimSpace(string(p.Type)), `"`)
		if strings.HasPrefix(typeStr, "[") { // union e.g. ["string","null"]
			var list []string
			if err := json.Unmarshal(p.Type, &list); err == nil {
				typeStr = strings.Join(list, " | ")
			}
		}
		def := strings.TrimSpace(string(p.Default))
		if def == "null" {
			def = ""
		}
		out[name] = schemaProp{
			Type:        typeStr,
			Description: p.Description,
			Default:     def,
			Required:    reqSet[name],
		}
	}
	return out, nil
}

// pluginSummary renders the hover markdown for a plugin block.
func pluginSummary(kind, name string, version int, capabilities []string, schemaJSON string) string {
	props, err := parseSchemaProperties(schemaJSON)
	if err != nil {
		return fmt.Sprintf("**%s** (%s plugin, v%d)", name, kind, version)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "**%s** — %s plugin (ABI v%d)", name, kind, version)
	if len(capabilities) > 0 {
		fmt.Fprintf(&b, " · capabilities: %s", strings.Join(capabilities, ", "))
	}
	b.WriteString("\n\n")
	names := make([]string, 0, len(props))
	for n := range props {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := props[n]
		flags := ""
		if p.Required {
			flags = " (required)"
		} else if p.Default != "" && p.Default != `""` {
			flags = " (default `" + strings.Trim(p.Default, `"`) + "`)"
		}
		fmt.Fprintf(&b, "- `%s`%s: %s", n, flags, p.Type)
		if p.Description != "" {
			fmt.Fprintf(&b, " — %s", p.Description)
		}
		b.WriteString("\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}
