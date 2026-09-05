// Command sitegen renders the Eventboat documentation site: a static,
// zero-CDN HTML tree (every page flat in one directory plus assets/style.css)
// sized for GitHub Pages project hosting, where the site is served under a
// subpath (https://eventboat.github.io/eventboat/) and therefore every URL —
// stylesheet, cross-page links, everything — must stay relative.
//
// It renders three groups of pages:
//
//   - site/content/index.md          -> index.html (hero landing layout)
//   - docs/developer/*.md            -> guide pages, ordered by the YAML
//     frontmatter `order:` key (fallback: filename), with prev/next pager
//   - docs/*.md (excluding
//     naming-checklist.md)           -> reference pages, second sidebar group
//
// Relative links that point outside the rendered page set (e.g.
// ../redesign-v3.md from docs/plugins.md) are rewritten to absolute GitHub
// URLs against main; links between rendered pages become ./<slug>.html.
//
// Usage — sitegen is a nested module (like examples/plugins/ticker-source,
// keeping site dependencies out of the main go.mod), so `go run
// ./tools/sitegen` from the repo root does NOT work (the root module does
// not contain the package). Run it from inside the module directory:
//
//	cd tools/sitegen && go run . -src <repo-root> -out <out-dir>
//
// which is exactly what the Pages workflow does, or equivalently from the
// repo root: `go -C tools/sitegen run . -src . -out bin/site`.
package main

import (
	"bytes"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yuin/goldmark"
	meta "github.com/yuin/goldmark-meta"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer/html"
)

const (
	version    = "v0.1.0-beta"
	githubBase = "https://github.com/eventboat/eventboat"
	rawBase    = "https://raw.githubusercontent.com/eventboat/eventboat/main"

	indexPath    = "site/content/index.md"
	guideDir     = "docs/developer"
	referenceDir = "docs"
	skipRefFile  = "naming-checklist.md"
)

//go:embed templates
var templateFS embed.FS

var markdown = goldmark.New(
	goldmark.WithExtensions(extension.GFM, meta.New()),
	goldmark.WithParserOptions(parser.WithAutoHeadingID()),
	// All rendered markdown is first-party repo content; raw HTML in it is
	// trusted (the landing page's card grid uses it).
	goldmark.WithRendererOptions(html.WithUnsafe()),
)

func main() {
	log.SetFlags(0)
	src := flag.String("src", ".", "repository root to render (must contain docs/ and site/content/)")
	out := flag.String("out", "bin/site", "output directory, relative to the current directory")
	ver := flag.String("version", version, "version string shown in the site header")
	flag.Parse()

	if err := run(*src, *out, *ver); err != nil {
		log.Fatalf("sitegen: %v", err)
	}
}

func run(srcDir, outDir, ver string) error {
	site, err := loadSite(srcDir)
	if err != nil {
		return err
	}
	site.renderBodies()
	if err := emit(outDir, site, ver); err != nil {
		return err
	}
	return linkCheck(outDir)
}

// emit renders every page flat into outDir (so all pages share the same
// relative prefix "assets/style.css" and cross links "./<slug>.html") and
// writes the stylesheet.
func emit(outDir string, s *site, ver string) error {
	tpl, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "assets"), 0o755); err != nil {
		return err
	}
	for _, p := range s.pages {
		name := "doc"
		if p.Group == groupIndex {
			name = "index"
		}
		data := &navData{
			Title:        p.DisplayTitle(),
			Version:      ver,
			Body:         p.body,
			Guides:       s.guides,
			References:   s.references,
			Prev:         p.prev,
			Next:         p.next,
			ProjectLinks: projectLinks,
		}
		p.Active = true // highlight the current page in the sidebar
		var buf bytes.Buffer
		if err := tpl.ExecuteTemplate(&buf, name, data); err != nil {
			return fmt.Errorf("render %s: %w", p.Source, err)
		}
		p.Active = false
		target := filepath.Join(outDir, p.Slug+".html")
		if err := os.WriteFile(target, buf.Bytes(), 0o644); err != nil {
			return err
		}
		fmt.Printf("wrote %s\n", filepath.ToSlash(target))
	}
	css, err := templateFS.ReadFile("templates/style.css")
	if err != nil {
		return err
	}
	cssTarget := filepath.Join(outDir, "assets", "style.css")
	if err := os.WriteFile(cssTarget, css, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", filepath.ToSlash(cssTarget))
	return nil
}

var navAttrRe = regexp.MustCompile(`\b(href|src)="([^"]*)"`)

// linkCheck verifies that every relative href/src in the emitted HTML
// resolves to another emitted page or a file in the output tree. Absolute
// URLs (the rewritten GitHub links) and pure anchors are skipped.
func linkCheck(outDir string) error {
	root := filepath.ToSlash(outDir)
	var broken []string
	err := filepath.WalkDir(outDir, func(f string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(f, ".html") {
			return err
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		page := strings.TrimPrefix(filepath.ToSlash(f), root+"/")
		for _, m := range navAttrRe.FindAllStringSubmatch(string(raw), -1) {
			href := m[2]
			if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") ||
				strings.HasPrefix(href, "#") || strings.HasPrefix(href, "mailto:") {
				continue
			}
			target := href
			if i := strings.IndexByte(target, '#'); i >= 0 {
				target = target[:i]
			}
			if target == "" {
				continue
			}
			clean := pathClean(pathJoin(pathDir(page), target))
			if strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
				broken = append(broken, fmt.Sprintf("  %s: %s escapes the output tree", page, href))
				continue
			}
			if _, err := os.Stat(filepath.Join(outDir, filepath.FromSlash(clean))); err != nil {
				broken = append(broken, fmt.Sprintf("  %s: %s -> no such output file", page, href))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(broken) > 0 {
		return fmt.Errorf("broken internal links:\n%s", strings.Join(broken, "\n"))
	}
	fmt.Println("link check: all internal links resolve")
	return nil
}
