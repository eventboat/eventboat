package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Hugo output mode (-hugo-out): instead of the standalone HTML site, emit the
// developer guides as Hugo content pages for the organization site
// (eventboat/eventboat.github.io), whose docs section lives at
// content/docs/<section>/. The org workflow renders these at build time from
// the public engine repo, so publishing stays single-source: this repo owns
// the markdown, the Hugo site only consumes it.
//
// URLs: content/docs/developer/02-engine.md -> /docs/developer/02-engine/ —
// so same-directory links among the guides ("02-engine.md#x") rewrite to
// ("../02-engine/#x"). The frontmatter `order:` becomes Hugo `weight` (×10,
// leaving room for insertions), `title:` carries over, and `linkTitle` is the
// title up to its first " & " ("Architecture & package map" -> "Architecture")
// to keep the sidebar readable. A leading body heading is dropped — Hugo
// themes render the frontmatter title, and a body <h1> would duplicate it.

const (
	hugoSection      = "developer"
	hugoSectionLink  = "Developer"
	hugoSectionTitle = "Developer guide"
	hugoSectionDesc  = "Architecture, engine internals, the plugin system, and the contribution workflow — for engineers building Eventboat itself or compiling in plugins."
	hugoSectionIntro = `These guides are rendered at build time from the
[eventboat/eventboat](https://github.com/eventboat/eventboat) repository;
edit them there, under docs/developer/.`
)

// guideLinkRe matches links between the guides themselves, with an optional
// fragment: "02-engine.md", "02-engine.md#commit-frontier".
var guideLinkRe = regexp.MustCompile(`\]\(([0-9]{2}-[a-z0-9-]+)\.md(#[^)\s]*)?\)`)

var frontmatterRe = regexp.MustCompile(`(?s)\A---\n.*?\n---\n`)
var leadingH1Re = regexp.MustCompile(`\A#+ [^\n]+\n+`)

// emitHugo writes the guide set as Hugo content into
// <outDir>/<hugoSection>/.
func emitHugo(outDir string, s *site) error {
	secDir := filepath.Join(outDir, hugoSection)
	if err := os.MkdirAll(secDir, 0o755); err != nil {
		return err
	}

	guides := append([]*page(nil), s.guides...)
	sort.Slice(guides, func(i, j int) bool { return guides[i].Order < guides[j].Order })

	index := fmt.Sprintf(`---
title: %s
linkTitle: %s
weight: 40
description: >-
    %s
---

%s
`, hugoSectionTitle, hugoSectionLink, hugoSectionDesc, hugoSectionIntro)
	if err := os.WriteFile(filepath.Join(secDir, "_index.md"), []byte(index), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", filepath.ToSlash(filepath.Join(secDir, "_index.md")))

	for _, p := range guides {
		if !frontmatterRe.Match(p.src) {
			return fmt.Errorf("%s: missing frontmatter", p.Source)
		}
		body := guideLinkRe.ReplaceAllString(string(frontmatterRe.ReplaceAll(p.src, nil)), "](../$1/$2)")
		body = leadingH1Re.ReplaceAllString(strings.TrimSpace(body), "")
		body = strings.TrimSpace(body)
		page := fmt.Sprintf(`---
title: %s
linkTitle: %s
weight: %d
---

%s
`, p.DisplayTitle(), hugoLinkTitle(p.DisplayTitle()), p.Order*10, body)
		target := filepath.Join(secDir, p.Slug+".md")
		if err := os.WriteFile(target, []byte(page), 0o644); err != nil {
			return err
		}
		fmt.Printf("wrote %s\n", filepath.ToSlash(target))
	}
	return nil
}

// hugoLinkTitle shortens a guide title for the sidebar: the part before the
// first " & ", or the full title when there is none.
func hugoLinkTitle(title string) string {
	if i := strings.Index(title, " &"); i >= 0 {
		return title[:i]
	}
	return title
}
