package extract

import (
	"bytes"
	"net/url"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

type anchor struct {
	href string
	rel  string
	node *html.Node
}

type metaTag struct {
	name    string // lowercased name, property or http-equiv
	content string
}

// page is the result of one walk over the parsed document. Every extractor
// reads from it, so the HTML is tokenized, parsed and traversed exactly once.
type page struct {
	raw    []byte
	root   *html.Node
	url    *url.URL
	base   *url.URL
	host   string
	domain string

	text      string // visible text, block elements separated by newlines
	title     string
	h1        string
	lang      string
	metas     []metaTag
	anchors   []anchor
	srcs      []string // script src and link href values
	jsonld    []string
	cfemails  []string
	addresses []*html.Node
	// footerStart is the offset in text where the first <footer> begins, or -1.
	footerStart int
}

func attr(n *html.Node, key string) string {
	for i := range n.Attr {
		if n.Attr[i].Key == key {
			return n.Attr[i].Val
		}
	}
	return ""
}

func hasAttr(n *html.Node, key string) bool {
	for i := range n.Attr {
		if n.Attr[i].Key == key {
			return true
		}
	}
	return false
}

// isBlock reports whether an element forces a line break in visible text.
func isBlock(a atom.Atom) bool {
	switch a {
	case atom.P, atom.Div, atom.Br, atom.Li, atom.Ul, atom.Ol, atom.Tr, atom.Td, atom.Th, atom.Table,
		atom.H1, atom.H2, atom.H3, atom.H4, atom.H5, atom.H6, atom.Section, atom.Article,
		atom.Header, atom.Footer, atom.Nav, atom.Aside, atom.Address, atom.Blockquote,
		atom.Dd, atom.Dt, atom.Dl, atom.Figure, atom.Figcaption, atom.Form, atom.Hr, atom.Main,
		atom.Pre, atom.Body, atom.Title:
		return true
	}
	return false
}

// isHidden reports whether an element's subtree is never rendered as text.
func isHidden(a atom.Atom) bool {
	switch a {
	case atom.Script, atom.Style, atom.Noscript, atom.Template, atom.Svg, atom.Iframe,
		atom.Head, atom.Select, atom.Textarea, atom.Object, atom.Canvas:
		return true
	}
	return false
}

// maxTextBytes caps the visible text kept per page; anything beyond it is
// boilerplate for the purposes of contact extraction.
const maxTextBytes = 512 << 10

func walk(raw []byte, root *html.Node, u *url.URL) *page {
	p := &page{raw: raw, root: root, url: u, base: u, footerStart: -1}
	var sb textBuf
	sb.Grow(len(raw) / 6)

	var visit func(n *html.Node, hidden bool)
	visit = func(n *html.Node, hidden bool) {
		switch n.Type {
		case html.TextNode:
			if !hidden && sb.Len() < maxTextBytes {
				sb.write(n.Data)
			}
			return
		case html.ElementNode:
			switch n.DataAtom {
			case atom.Html:
				p.lang = strings.ToLower(attr(n, "lang"))
			case atom.Title:
				if p.title == "" && n.Parent != nil && n.Parent.DataAtom == atom.Head {
					p.title = collapse(textOf(n))
				}
			case atom.Base:
				if href := attr(n, "href"); href != "" {
					if b, err := u.Parse(href); err == nil && (b.Scheme == "http" || b.Scheme == "https") {
						p.base = b
					}
				}
			case atom.Meta:
				name := attr(n, "name")
				if name == "" {
					name = attr(n, "property")
				}
				if name == "" {
					name = attr(n, "http-equiv")
				}
				if name != "" {
					p.metas = append(p.metas, metaTag{strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(attr(n, "content"))})
				}
			case atom.Script:
				if src := attr(n, "src"); src != "" {
					p.srcs = append(p.srcs, src)
				} else if strings.Contains(strings.ToLower(attr(n, "type")), "ld+json") && n.FirstChild != nil {
					p.jsonld = append(p.jsonld, n.FirstChild.Data)
				}
			case atom.Link:
				if href := attr(n, "href"); href != "" {
					p.srcs = append(p.srcs, href)
				}
			case atom.A:
				if href := strings.TrimSpace(attr(n, "href")); href != "" {
					p.anchors = append(p.anchors, anchor{href: href, rel: strings.ToLower(attr(n, "rel")), node: n})
				}
			case atom.Address:
				p.addresses = append(p.addresses, n)
			case atom.H1:
				if p.h1 == "" {
					p.h1 = collapse(textOf(n))
				}
			case atom.Footer:
				if p.footerStart < 0 {
					p.footerStart = sb.Len()
				}
			}
			if cf := attr(n, "data-cfemail"); cf != "" {
				p.cfemails = append(p.cfemails, cf)
			}
			if !hidden && isHidden(n.DataAtom) {
				hidden = true
			}
			if !hidden && (hasAttr(n, "hidden") || attr(n, "aria-hidden") == "true") {
				// Still walk for links and metadata, but keep the text out.
				hidden = true
			}
			block := !hidden && isBlock(n.DataAtom)
			if block {
				sb.lineBreak()
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				visit(c, hidden)
			}
			if block {
				sb.lineBreak()
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			visit(c, hidden)
		}
	}
	visit(root, false)
	p.text = sb.String()
	p.host = u.Hostname()
	return p
}

// textBuf accumulates visible text, folding whitespace runs to one space and
// block boundaries to one newline.
type textBuf struct {
	strings.Builder
	last byte // last byte written: 0 (empty), ' ', '\n' or other
}

func (t *textBuf) space() {
	if t.last != 0 && t.last != ' ' && t.last != '\n' {
		t.WriteByte(' ')
		t.last = ' '
	}
}

func (t *textBuf) lineBreak() {
	if t.last == 0 || t.last == '\n' {
		return
	}
	t.WriteByte('\n')
	t.last = '\n'
}

func (t *textBuf) write(s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == ' ' || c == '\n' || c == '\t' || c == '\r' || c == '\f':
			t.space()
		case c == 0xC2 && i+1 < len(s) && s[i+1] == 0xA0: // U+00A0
			t.space()
			i++
		default:
			t.WriteByte(c)
			t.last = c
		}
	}
}

// textOf returns the concatenated visible text under n.
func textOf(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}
	var sb strings.Builder
	var rec func(*html.Node)
	rec = func(c *html.Node) {
		for ch := c.FirstChild; ch != nil; ch = ch.NextSibling {
			switch ch.Type {
			case html.TextNode:
				sb.WriteString(ch.Data)
			case html.ElementNode:
				if isHidden(ch.DataAtom) {
					continue
				}
				block := isBlock(ch.DataAtom)
				if block {
					sb.WriteByte('\n')
				}
				rec(ch)
				if block {
					sb.WriteByte('\n')
				}
			}
		}
	}
	rec(n)
	return sb.String()
}

// collapse folds all whitespace (including NBSP) to single spaces and trims.
func collapse(s string) string {
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", " ")
	return strings.Join(strings.Fields(s), " ")
}

// lines splits block text into trimmed non-empty lines.
func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = collapse(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

func (p *page) meta(names ...string) string {
	for _, want := range names {
		for _, m := range p.metas {
			if m.name == want && m.content != "" {
				return m.content
			}
		}
	}
	return ""
}

// TextDensity is a cheap pre-parse estimate used to decide whether a page
// needs JavaScript rendering: bytes of visible text, and the number of
// scripts that load code (external or module scripts). Small inline snippets
// such as analytics tags do not build a page and are not counted.
func TextDensity(body []byte) (textBytes, scripts int) {
	z := html.NewTokenizer(bytes.NewReader(body))
	skip := 0
	for {
		switch z.Next() {
		case html.ErrorToken:
			return textBytes, scripts
		case html.StartTagToken:
			name, hasAttr := z.TagName()
			switch atom.Lookup(name) {
			case atom.Script:
				skip++
				for hasAttr {
					var k, v []byte
					k, v, hasAttr = z.TagAttr()
					if (string(k) == "src" && len(v) > 0) || (string(k) == "type" && string(v) == "module") {
						scripts++
						break
					}
				}
			case atom.Style, atom.Noscript, atom.Template, atom.Svg, atom.Title:
				skip++
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			switch atom.Lookup(name) {
			case atom.Script, atom.Style, atom.Noscript, atom.Template, atom.Svg, atom.Title:
				if skip > 0 {
					skip--
				}
			}
		case html.TextToken:
			if skip == 0 {
				textBytes += len(bytes.TrimSpace(z.Text()))
			}
		}
	}
}
