package main

import (
	"bytes"
	"fmt"
	"html"
	"html/template"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	meta "github.com/yuin/goldmark-meta"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

type group int

const (
	groupIndex group = iota
	groupGuide
	groupReference
)

// page is one markdown source rendered into one flat HTML file.
type page struct {
	Slug   string // output file base name (no extension)
	Source string // repo-relative, slash-separated source path
	Group  group
	Title  string // frontmatter title, or "" until the leading <h1> is mined
	Order  int
	// orderWasSet is true when the frontmatter carried a numeric `order:`;
	// pages without it sort after ordered ones, by filename.
	orderWasSet bool
	layout      string // frontmatter `layout:` (only "index" is special)

	doc    ast.Node // parsed document, links rewritten before render
	src    []byte   // raw markdown
	body   template.HTML
	prev   *page // pager across the Guide group only
	next   *page
	Active bool // current page while its own HTML is being rendered
}

// DisplayTitle is the sidebar/nav label: the page title, falling back to the
// slug for files with neither frontmatter title nor a leading heading.
func (p *page) DisplayTitle() string {
	if p.Title != "" {
		return p.Title
	}
	return p.Slug
}

// projLink is one entry of the static "Project" sidebar group.
type projLink struct {
	Label string
	Href  string
}

var projectLinks = []projLink{
	{"GitHub", githubBase},
	{"CHANGELOG", githubBase + "/blob/main/CHANGELOG.md"},
	{"Design spec (redesign-v3)", githubBase + "/blob/main/redesign-v3.md"},
	{"README (中文)", githubBase + "/blob/main/README_ZH.md"},
}

// navData is the template payload for every page.
type navData struct {
	Title        string
	Version      string
	Body         template.HTML
	Guides       []*page
	References   []*page
	Prev         *page
	Next         *page
	ProjectLinks []projLink
}

type site struct {
	pages      []*page // all emitted pages, index first
	bySource   map[string]*page
	bySlug     map[string]*page
	guides     []*page // sorted, pager wired
	references []*page // sorted
	index      *page
}

// loadSite discovers and parses every rendered page. The guide and reference
// sets are auto-discovered — whatever .md files exist at render time are the
// site.
func loadSite(srcDir string) (*site, error) {
	s := &site{
		bySource: map[string]*page{},
		bySlug:   map[string]*page{},
	}
	for _, f := range globMD(srcDir, guideDir) {
		s.tryAdd(srcDir, f, groupGuide)
	}
	for _, f := range globMD(srcDir, referenceDir) {
		if strings.EqualFold(filepath.Base(f), skipRefFile) {
			continue // internal working notes, not site content
		}
		s.tryAdd(srcDir, f, groupReference)
	}
	idx := filepath.Join(srcDir, filepath.FromSlash(indexPath))
	if _, err := os.Stat(idx); err != nil {
		wd, _ := os.Getwd()
		return nil, fmt.Errorf("%s not found under -src %s (cwd %s)", indexPath, srcDir, wd)
	}
	s.tryAdd(srcDir, idx, groupIndex)

	s.sortGroups()
	s.wirePager()
	return s, nil
}

func globMD(srcDir, rel string) []string {
	matches, _ := filepath.Glob(filepath.Join(srcDir, filepath.FromSlash(rel), "*.md"))
	sort.Strings(matches)
	return matches
}

// tryAdd parses one markdown file into a page; parse errors are fatal, but a
// slug collision only warns (the first page with a slug wins, the loser's
// links fall through to GitHub URLs).
func (s *site) tryAdd(srcDir, file string, g group) {
	raw, err := os.ReadFile(file)
	if err != nil {
		logFatalf("read %s: %v", file, err)
	}
	rel, err := filepath.Rel(srcDir, file)
	if err != nil {
		logFatalf("resolve %s: %v", file, err)
	}
	rel = filepath.ToSlash(rel)
	slug := strings.TrimSuffix(path.Base(rel), ".md")
	if _, dup := s.bySlug[slug]; dup {
		logPrintf("sitegen: warning: duplicate slug %q (%s); rendering the first only", slug, rel)
		return
	}

	ctx := parser.NewContext()
	doc := markdown.Parser().Parse(text.NewReader(raw), parser.WithContext(ctx))
	p := &page{
		Slug:   slug,
		Source: rel,
		Group:  g,
		doc:    doc,
		src:    raw,
	}
	for k, v := range meta.Get(ctx) {
		val := strings.TrimSpace(fmt.Sprintf("%v", v))
		switch k {
		case "title":
			p.Title = val
		case "layout":
			p.layout = val
		case "order":
			if n, err := strconv.Atoi(val); err == nil {
				p.Order, p.orderWasSet = n, true
			} else {
				logPrintf("sitegen: warning: %s: non-numeric order %q ignored (sorting by filename)", rel, val)
			}
		}
	}
	s.pages = append(s.pages, p)
	s.bySource[rel] = p
	s.bySlug[slug] = p
	switch g {
	case groupGuide:
		s.guides = append(s.guides, p)
	case groupReference:
		s.references = append(s.references, p)
	case groupIndex:
		s.index = p
	}
}

// sortGroups orders the Guide group by frontmatter order (fallback filename)
// and the Reference group by filename.
func (s *site) sortGroups() {
	sort.SliceStable(s.guides, func(i, j int) bool {
		a, b := s.guides[i], s.guides[j]
		if a.orderWasSet != b.orderWasSet {
			return a.orderWasSet
		}
		if a.orderWasSet && a.Order != b.Order {
			return a.Order < b.Order
		}
		return a.Slug < b.Slug
	})
	sort.SliceStable(s.references, func(i, j int) bool { return s.references[i].Slug < s.references[j].Slug })
}

// wirePager chains prev/next across the Guide group, in display order.
func (s *site) wirePager() {
	for i, p := range s.guides {
		if i > 0 {
			p.prev = s.guides[i-1]
		}
		if i+1 < len(s.guides) {
			p.next = s.guides[i+1]
		}
	}
}

var (
	h1Re  = regexp.MustCompile(`(?s)<h1(\s[^>]*)?>(.*?)</h1>`)
	tagRe = regexp.MustCompile(`<[^>]*>`)
)

// renderBodies rewrites links in each parsed document, renders it to HTML and
// mines/dedupes the leading <h1>: pages without a frontmatter title take it
// from the heading (and drop the heading, the layout re-emits <h1>); pages
// whose body repeats the frontmatter title lose the duplicate heading.
func (s *site) renderBodies() {
	for _, p := range s.pages {
		rewriteLinks(p, s)
		var buf bytes.Buffer
		if err := markdown.Renderer().Render(&buf, p.src, p.doc); err != nil {
			logFatalf("render %s: %v", p.Source, err)
		}
		body := buf.String()
		if m := h1Re.FindStringSubmatchIndex(body); m != nil && strings.TrimSpace(body[:m[0]]) == "" {
			// m[4]:m[5] is the inner-HTML group (m[2]:m[3] is the attrs);
			// goldmark escapes entities in heading text ("&" → "&amp;"),
			// so unescape before comparing against the frontmatter title.
			text := strings.TrimSpace(html.UnescapeString(tagRe.ReplaceAllString(body[m[4]:m[5]], "")))
			if p.Title == "" {
				p.Title = text
				body = body[:m[0]] + body[m[1]:]
			} else if normalizeTitle(p.Title) == normalizeTitle(text) {
				body = body[:m[0]] + body[m[1]:]
			}
		}
		if p.Group == groupIndex {
			body = fixupLandingCards(body, s)
		}
		p.body = template.HTML(body)
	}
}

func normalizeTitle(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// logPrintf / logFatalf keep the "sitegen: " prefix consistent from helpers.
func logPrintf(format string, args ...any) { log.Printf("sitegen: "+format, args...) }
func logFatalf(format string, args ...any) { log.Fatalf("sitegen: "+format, args...) }
