package main

import (
	"path"
	"regexp"
	"strings"

	"github.com/yuin/goldmark/ast"
)

// linkRewriteRules, in the order they are consulted for a relative link found
// in a rendered markdown file (destinations are resolved against the source
// file's directory, giving a repo-relative path):
//
//  1. Absolute URLs (http/https/mailto) and pure in-page anchors (#...) pass
//     through untouched.
//  2. A repo-relative path that IS a rendered page (docs/developer/01-x.md,
//     docs/plugins.md, site/content/index.md) becomes "./<slug>.html" — flat
//     output means every cross-page link is one segment deep from any page.
//  3. A .md link whose FILE NAME matches a rendered slug does the same. This
//     is how the landing page's hardcoded guide links resolve even though
//     site/content/index.md sits in a different directory.
//  4. Anything else escapes the rendered set (../redesign-v3.md,
//     ../examples/k8s/deployment.yaml, docs/naming-checklist.md) and becomes
//     an absolute GitHub URL against main (images: raw.githubusercontent),
//     keeping any #fragment. This is what keeps the site honest while it
//     covers only part of the repository.
//
// Rule 3 logs a note when it fires, and rule 4 logs when a .md target looks
// like it was meant to be a rendered page (a numbered guide), so a filename
// mismatch between the landing page and docs/developer/ is visible in CI.

const (
	parsedNote = "note: %s links %q -> not a rendered page; using a GitHub URL"
)

// rewriteLinks mutates the link/image destinations of a parsed document.
func rewriteLinks(p *page, s *site) {
	dir := path.Dir(p.Source)
	ast.Walk(p.doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch l := n.(type) {
		case *ast.Link:
			l.Destination = []byte(rewriteDest(string(l.Destination), p, dir, s, false))
		case *ast.Image:
			l.Destination = []byte(rewriteDest(string(l.Destination), p, dir, s, true))
		}
		return ast.WalkContinue, nil
	})
}

func rewriteDest(dest string, p *page, dir string, s *site, isImage bool) string {
	d := strings.TrimSpace(dest)
	if d == "" || strings.HasPrefix(d, "#") ||
		strings.HasPrefix(d, "http://") || strings.HasPrefix(d, "https://") ||
		strings.HasPrefix(d, "mailto:") {
		return dest
	}
	frag := ""
	if i := strings.IndexByte(d, '#'); i >= 0 {
		frag, d = d[i:], d[:i]
	}
	if d == "" {
		return dest
	}
	// Links already written against the flat output (e.g. authored .html
	// links) normalize to a single segment so they work from any page.
	if strings.HasSuffix(d, ".html") {
		return "./" + path.Base(d) + frag
	}
	resolved := path.Clean(path.Join(dir, d))
	if pg, ok := s.bySource[resolved]; ok {
		return "./" + pg.Slug + ".html" + frag
	}
	if strings.HasSuffix(resolved, ".md") {
		base := strings.TrimSuffix(path.Base(resolved), ".md")
		if _, ok := s.bySlug[base]; ok {
			logPrintf(parsedNote+" (resolved by slug)", p.Source, dest)
			return "./" + base + ".html" + frag
		}
		if isGuideSlug(base) {
			logPrintf("note: %s links %q -> no guide with that slug is rendered; using a GitHub URL", p.Source, dest)
		} else {
			logPrintf(parsedNote, p.Source, dest)
		}
	}
	if isImage {
		return rawBase + "/" + resolved + frag
	}
	return githubBase + "/blob/main/" + resolved + frag
}

var cardHrefRe = regexp.MustCompile(`href="([0-9]{2}-[a-z0-9-]+)\.html"`)

// fixupLandingCards upgrades or downgrades the landing page's hardcoded card
// links. Raw-HTML card hrefs skip the markdown-level rewriter, so they are
// checked here against the actually rendered set: slugs that exist keep their
// internal .html href; slugs that do not (guides not landed yet, or renamed)
// are pointed at the GitHub blob URL for docs/developer/<slug>.md so the card
// grid never dead-links, at the cost of leaving the site.
func fixupLandingCards(body string, s *site) string {
	return cardHrefRe.ReplaceAllStringFunc(body, func(m string) string {
		slug := cardHrefRe.FindStringSubmatch(m)[1]
		if _, ok := s.bySlug[slug]; ok {
			return m
		}
		logPrintf("note: landing card %q not rendered yet; linking to GitHub until the guide lands", slug)
		return `href="` + githubBase + `/blob/main/` + guideDir + `/` + slug + `.md"`
	})
}

func isGuideSlug(slug string) bool {
	return len(slug) > 2 && slug[2] == '-' && slug[0] >= '0' && slug[0] <= '9' && slug[1] >= '0' && slug[1] <= '9'
}

// Small indirections over path.* so main.go's link check stays readable.
func pathDir(p string) string        { return path.Dir(p) }
func pathJoin(elem ...string) string { return path.Join(elem...) }
func pathClean(p string) string      { return path.Clean(p) }
