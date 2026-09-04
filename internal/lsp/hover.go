package lsp

import (
	"encoding/json"
	"strings"
)

// hover answers textDocument/hover: the token under the cursor resolved
// against the same authorities as completion (registry catalog, plugin
// schemas, framework-field descriptions).
func (s *Server) hover(params []byte) (any, *ResponseError) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position struct {
			Line      int `json:"line"`
			Character int `json:"character"`
		} `json:"position"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &ResponseError{Code: codeInvalidRequest, Message: err.Error()}
	}
	text, _ := s.document(p.TextDocument.URI)
	value := s.hoverValue(text, p.Position.Line, p.Position.Character)
	if value == "" {
		return nil, nil // no hover content
	}
	return map[string]any{
		"contents": map[string]any{"kind": "markdown", "value": value},
	}, nil
}

// hoverValue extracts the word at the position and resolves it.
func (s *Server) hoverValue(text string, line, character int) string {
	lines := strings.Split(text, "\n")
	if line < 0 || line >= len(lines) {
		return ""
	}
	l := lines[line]
	if character < 0 || character > len(l) {
		return ""
	}
	// Word span around the cursor: identifier chars or `-`.
	start := character
	for start > 0 && isHoverWordByte(l[start-1]) {
		start--
	}
	end := character
	for end < len(l) && isHoverWordByte(l[end]) {
		end++
	}
	if start == end {
		return ""
	}
	word := l[start:end]
	stack, _, _ := analyze(lines, line, character)
	_ = stack

	// Framework fields first (they exist in every node).
	if doc, ok := frameworkDocs[word]; ok {
		return "`" + word + "` — " + doc
	}

	// Plugin names, by the section the cursor is in.
	section := sectionOfStack(stack)
	switch section {
	case "sources":
		if m, ok := s.reg.LookupSource(word); ok {
			return pluginSummary("source", m.Name, m.Version, m.Capabilities, m.Schema)
		}
	case "sinks":
		if m, ok := s.reg.LookupSink(word); ok {
			return pluginSummary("sink", m.Name, m.Version, nil, m.Schema)
		}
	}

	// A field inside a plugin block: find the enclosing plugin key.
	if section != "" {
		if plugin := enclosingPlugin(stack, section); plugin != "" {
			var schema string
			switch section {
			case "sources":
				if m, ok := s.reg.LookupSource(plugin); ok {
					schema = m.Schema
				}
			case "sinks":
				if m, ok := s.reg.LookupSink(plugin); ok {
					schema = m.Schema
				}
			}
			if props, err := parseSchemaProperties(schema); err == nil {
				if p, ok := props[word]; ok {
					out := "`" + word + "`: " + p.Type
					if p.Required {
						out += " (required)"
					}
					if p.Description != "" {
						out += " — " + p.Description
					}
					return out
				}
			}
		}
	}
	return ""
}

func isHoverWordByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9', b == '_', b == '-':
		return true
	}
	return false
}

func sectionOfStack(stack []stackEntry) string {
	for _, e := range stack {
		if e.indent == 0 && isSection(e.key) {
			return e.key
		}
	}
	return ""
}

// enclosingPlugin finds the nearest non-framework key under a node — the
// plugin block the cursor is inside ("" when none).
func enclosingPlugin(stack []stackEntry, section string) string {
	framework := frameworkFields[section]
	isFramework := func(k string) bool {
		for _, f := range framework {
			if f == k {
				return true
			}
		}
		return false
	}
	for i := len(stack) - 1; i >= 0; i-- {
		e := stack[i]
		if e.indent == 0 {
			break
		}
		if isSection(e.key) {
			continue
		}
		if !isFramework(e.key) {
			return e.key
		}
	}
	return ""
}
