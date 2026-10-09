package extract

import (
	"bytes"
	_ "embed"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/appu/leadgraph/pkg/model"
)

//go:embed rules/tech.yaml
var defaultTechYAML []byte

// TechRule is one technology fingerprint. A rule matches when any of its
// patterns matches.
type TechRule struct {
	Name     string `yaml:"name"`
	Category string `yaml:"category"`
	// HTML holds case-sensitive substrings searched in the raw document.
	HTML []string `yaml:"html"`
	// Src holds regular expressions matched against script src / link href.
	Src []string `yaml:"src"`
	// Meta maps a meta name to a regular expression for its content.
	Meta map[string]string `yaml:"meta"`
	// Header maps a response header to a regular expression for its value.
	Header map[string]string `yaml:"header"`

	html   [][]byte
	src    []*regexp.Regexp
	meta   map[string]*regexp.Regexp
	header map[string]*regexp.Regexp
}

// TechRules is a compiled rule set.
type TechRules struct{ rules []TechRule }

// Len returns the number of rules.
func (t *TechRules) Len() int { return len(t.rules) }

// ParseTechRules compiles a YAML rule set.
func ParseTechRules(data []byte) (*TechRules, error) {
	var rules []TechRule
	if err := yaml.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("tech rules: %w", err)
	}
	for i := range rules {
		r := &rules[i]
		if r.Name == "" {
			return nil, fmt.Errorf("tech rules: rule %d has no name", i)
		}
		for _, h := range r.HTML {
			r.html = append(r.html, []byte(h))
		}
		for _, s := range r.Src {
			re, err := regexp.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("tech rules: %s: %w", r.Name, err)
			}
			r.src = append(r.src, re)
		}
		r.meta = map[string]*regexp.Regexp{}
		for k, s := range r.Meta {
			re, err := regexp.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("tech rules: %s: %w", r.Name, err)
			}
			r.meta[strings.ToLower(k)] = re
		}
		r.header = map[string]*regexp.Regexp{}
		for k, s := range r.Header {
			re, err := regexp.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("tech rules: %s: %w", r.Name, err)
			}
			r.header[strings.ToLower(k)] = re
		}
	}
	return &TechRules{rules: rules}, nil
}

// DefaultTechRules returns the built-in rule set.
var DefaultTechRules = func() *TechRules {
	r, err := ParseTechRules(defaultTechYAML)
	if err != nil {
		panic(err)
	}
	return r
}()

func (st *state) extractTech(rules *TechRules) {
	p := st.p
	headers := map[string]string{}
	for k, v := range st.in.Headers {
		headers[strings.ToLower(k)] = strings.Join(v, ", ")
	}
	for i := range rules.rules {
		r := &rules.rules[i]
		conf, how := 0.0, ""
		for name, re := range r.header {
			if v, ok := headers[name]; ok && re.MatchString(v) {
				conf, how = 0.9, "header"
				break
			}
		}
		if conf == 0 {
			for name, re := range r.meta {
				if v := p.meta(name); v != "" && re.MatchString(v) {
					conf, how = 0.9, "meta"
					break
				}
			}
		}
		if conf == 0 {
		srcs:
			for _, re := range r.src {
				for _, s := range p.srcs {
					if re.MatchString(s) {
						conf, how = 0.85, "src"
						break srcs
					}
				}
			}
		}
		if conf == 0 {
			for _, h := range r.html {
				if bytes.Contains(p.raw, h) {
					conf, how = 0.7, "html"
					break
				}
			}
		}
		if conf > 0 {
			st.out.Technologies = append(st.out.Technologies, model.TechFact{
				Fact: st.fact(r.Name, "tech."+how, conf), Category: r.Category,
			})
		}
	}
}
