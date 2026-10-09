// Package parse is the parse stage: it reads raw records, runs the pure
// extractors, stores the extraction and feeds discovered links back to the
// frontier.
package parse

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"io"
	"strings"

	"golang.org/x/net/html/charset"
)

// ParseSitemap reads a sitemap. It understands <urlset>, <sitemapindex> and
// the plain-text format (one URL per line), and returns at most limit page
// URLs and limit child sitemaps. Malformed input yields whatever was read
// before the error.
func ParseSitemap(body []byte, limit int) (pages, sitemaps []string) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] != '<' {
		sc := bufio.NewScanner(bytes.NewReader(trimmed))
		for sc.Scan() && len(pages) < limit {
			line := strings.TrimSpace(sc.Text())
			if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
				pages = append(pages, line)
			}
		}
		return pages, nil
	}
	dec := xml.NewDecoder(bytes.NewReader(trimmed))
	dec.Strict = false
	dec.CharsetReader = charset.NewReaderLabel
	var stack []string
	for len(pages) < limit && len(sitemaps) < limit {
		tok, err := dec.Token()
		if err != nil {
			if err != io.EOF {
				// Keep what was parsed so far.
			}
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, strings.ToLower(t.Name.Local))
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			n := len(stack)
			if n < 2 || stack[n-1] != "loc" {
				continue
			}
			loc := strings.TrimSpace(string(t))
			if loc == "" {
				continue
			}
			switch stack[n-2] {
			case "url":
				pages = append(pages, loc)
			case "sitemap":
				sitemaps = append(sitemaps, loc)
			}
		}
	}
	return pages, sitemaps
}
