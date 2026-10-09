package extract

import (
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/pkg/model"
)

// roleWords are tokens that mark a string as a job title.
var roleWords = toSet(`ceo cto cfo coo cmo cio cpo cro cso ciso chro cdo chief founder cofounder
co-founder president vp svp evp avp director head manager lead engineer developer designer officer
partner principal consultant analyst architect scientist advisor adviser chairman chairwoman chair
owner executive specialist coordinator associate counsel attorney lawyer recruiter accountant
strategist editor researcher professor administrator representative assistant intern controller
treasurer secretary programmer technician supervisor evangelist ambassador marketer copywriter
geschäftsführer geschäftsführerin leiter leiterin gründer gründerin inhaber vorstand prokurist
directeur directrice gérant fondateur fondatrice responsable director directora gerente fundador
socio amministratore direttore titolare bestuurder oprichter`)

// nonNameWords never appear in a person's name but are common in headings.
var nonNameWords = toSet(`our the team about contact us meet leadership management board directors
company careers join read more learn get in touch privacy policy terms services products solutions
news blog press home customer customers success stories story case studies follow why what how who
we are open positions office view profile all rights reserved sign up log request demo free trial
latest new united states kingdom your you and or of for with to from at by on a an is it this that
welcome hello thank thanks support help center resources pricing features platform partners
investors mission vision values culture history overview quick links social media email phone
address location locations headquarters hq global worldwide international group inc ltd llc gmbh
limited corporation technologies technology software systems consulting digital agency studio
labs lab cloud data security marketing sales engineering product design finance legal operations
human people job jobs work working life here now today click subscribe newsletter cookie cookies
copyright menu search login logout account cart shop store book schedule call back top next
previous page pages section see explore discover download start started getting touch
executive advisory committee members member staff employees experts expert founders leaders
department departments industry industries business enterprise corporate general information
frequently asked questions faq st street avenue ave road rd suite floor building po box`)

var antiCard = []string{"testimonial", "review", "quote", "customer", "client", "comment", "speaker", "author", "byline", "post", "article"}

var cardWords = []string{"team", "member", "person", "people", "staff", "employee", "leader", "founder", "executive", "management", "board", "advisor", "bio", "profile"}

var titleClassWords = []string{"title", "role", "position", "job", "designation", "function"}

// plausibleName reports whether s (already cleaned) looks like a person's
// full name: two to five capitalized alphabetic tokens, none of which is a
// heading word or a role word.
func plausibleName(s string, _ bool) bool {
	if len(s) < 4 || len(s) > 60 {
		return false
	}
	tokens := strings.Fields(s)
	if len(tokens) < 2 || len(tokens) > 5 {
		return false
	}
	full := 0
	for i, tok := range tokens {
		low := strings.ToLower(strings.Trim(tok, ".,"))
		if low == "" {
			return false
		}
		if _, bad := nonNameWords[low]; bad {
			return false
		}
		if _, bad := roleWords[low]; bad {
			return false
		}
		letters := 0
		first := true
		for _, r := range tok {
			switch {
			case unicode.IsLetter(r):
				if first {
					if !unicode.IsUpper(r) && !(i > 0 && i < len(tokens)-1) {
						// Lowercase is only allowed for middle particles
						// ("van", "de").
						return false
					}
					first = false
				}
				letters++
			case r == '.' || r == '-' || r == '\'' || r == '’':
			default:
				return false
			}
		}
		if letters == 0 {
			return false
		}
		if letters >= 2 {
			full++
		}
	}
	// "J. R. Tolkien": initials are fine when a full surname closes the name.
	return full >= 2 || (len(tokens) >= 3 && full == 1 && len(strings.Trim(tokens[len(tokens)-1], ".")) >= 2)
}

// LooksLikeTitle reports whether s reads as a job title.
func LooksLikeTitle(s string) bool {
	if len(s) < 2 || len(s) > 120 {
		return false
	}
	words := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && r != '-'
	})
	if len(words) == 0 || len(words) > 14 {
		return false
	}
	if len(words) > 3 && strings.HasSuffix(s, ".") {
		return false // a sentence, not a title
	}
	for _, w := range words {
		if _, ok := roleWords[w]; ok {
			return true
		}
	}
	return false
}

type peopleSink struct {
	byKey map[string]int
	list  []model.PersonFact
}

func (s *peopleSink) add(pf model.PersonFact) {
	key := normalize.PersonKey(pf.Value)
	if key == "" || len(s.list) >= 300 {
		return
	}
	if i, ok := s.byKey[key]; ok {
		prev := &s.list[i]
		if pf.Confidence > prev.Confidence {
			pf.Emails = mergeStrings(pf.Emails, prev.Emails)
			pf.Phones = mergeStrings(pf.Phones, prev.Phones)
			pf.Socials = mergeSocials(pf.Socials, prev.Socials)
			if pf.Title == "" {
				pf.Title = prev.Title
			}
			*prev = pf
		} else {
			prev.Emails = mergeStrings(prev.Emails, pf.Emails)
			prev.Phones = mergeStrings(prev.Phones, pf.Phones)
			prev.Socials = mergeSocials(prev.Socials, pf.Socials)
			if prev.Title == "" {
				prev.Title = pf.Title
			}
		}
		return
	}
	s.byKey[key] = len(s.list)
	s.list = append(s.list, pf)
}

func mergeStrings(a, b []string) []string {
	for _, x := range b {
		found := false
		for _, y := range a {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			a = append(a, x)
		}
	}
	return a
}

func mergeSocials(a, b []model.SocialFact) []model.SocialFact {
	for _, x := range b {
		found := false
		for _, y := range a {
			if x.Value == y.Value {
				found = true
				break
			}
		}
		if !found {
			a = append(a, x)
		}
	}
	return a
}

func classAndID(n *html.Node) string {
	c, id := attr(n, "class"), attr(n, "id")
	if c == "" && id == "" {
		return ""
	}
	return strings.ToLower(c + " " + id)
}

func containsAny(s string, words []string) bool {
	if s == "" {
		return false
	}
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

func isNameClass(cls string) bool {
	if !strings.Contains(cls, "name") {
		return false
	}
	return !containsAny(cls, []string{"company", "user", "file", "site", "brand", "product", "org"})
}

func isNameElement(n *html.Node) bool {
	switch n.DataAtom {
	case atom.H1, atom.H2, atom.H3, atom.H4, atom.H5, atom.H6, atom.Strong, atom.B, atom.Dt, atom.Figcaption:
		return true
	}
	if attr(n, "itemprop") == "name" {
		return true
	}
	return isNameClass(classAndID(n))
}

type nameHit struct {
	node *html.Node
	name string
}

var nameTitleLine = regexp.MustCompile(`^(.{4,60}?)\s*(?:[,|–—•·]|\s-\s|:)\s*(.{2,110})$`)

func (st *state) extractPeople() {
	pt := st.out.PageType
	peoplePage := pt == model.PageTeam || pt == model.PageAbout || pt == model.PageContact

	// 1. Collect every element whose own text is a plausible name.
	var hits []nameHit
	var rows []*html.Node
	var lineNodes []*html.Node
	var collect func(n *html.Node, inName bool)
	collect = func(n *html.Node, inName bool) {
		if n.Type != html.ElementNode {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				collect(c, inName)
			}
			return
		}
		if isHidden(n.DataAtom) && n.DataAtom != atom.Head {
			return
		}
		switch n.DataAtom {
		case atom.Head, atom.Nav, atom.Form, atom.Button:
			return
		case atom.Tr:
			rows = append(rows, n)
		case atom.Li, atom.P:
			lineNodes = append(lineNodes, n)
		}
		if !inName && len(hits) < 400 && isNameElement(n) {
			if name := normalize.PersonName(firstLine(textOf(n))); plausibleName(name, false) {
				hits = append(hits, nameHit{n, name})
				inName = true
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			collect(c, inName)
		}
	}
	collect(st.p.root, false)

	// 2. For each ancestor (a few levels up) record which distinct names it
	// contains, so the largest single-person container can be found.
	const climb = 5
	names := map[*html.Node]map[string]struct{}{}
	for _, h := range hits {
		key := normalize.PersonKey(h.name)
		a := h.node.Parent
		for i := 0; i < climb && a != nil && a.Type == html.ElementNode && a.DataAtom != atom.Body; i++ {
			set := names[a]
			if set == nil {
				set = map[string]struct{}{}
				names[a] = set
			}
			set[key] = struct{}{}
			a = a.Parent
		}
	}

	captured := map[*html.Node]bool{}
	isHit := make(map[*html.Node]bool, len(hits))
	for _, h := range hits {
		isHit[h.node] = true
	}
	for _, h := range hits {
		var scope *html.Node
		card, anti, schema := false, false, false
		a := h.node.Parent
		for i := 0; i < climb+2 && a != nil && a.Type == html.ElementNode && a.DataAtom != atom.Body; i++ {
			cls := classAndID(a)
			if containsAny(cls, antiCard) || a.DataAtom == atom.Blockquote {
				anti = true
			}
			if i < climb && len(names[a]) == 1 {
				scope = a
				if containsAny(cls, cardWords) {
					card = true
				}
				if strings.Contains(attr(a, "itemtype"), "schema.org/Person") {
					schema = true
				}
			}
			a = a.Parent
		}
		if anti && !schema {
			continue
		}
		title := ""
		if scope != nil {
			title = findTitle(scope, h)
		} else {
			title = titleInSiblings(h.node, isHit, names)
		}
		title = st.ownTitle(title)
		var conf float64
		switch {
		case title != "" && (card || schema):
			conf = 0.8
		case title != "" && peoplePage:
			conf = 0.7
		case title == "" && (card || schema) && peoplePage && scope != nil:
			conf = 0.5
		default:
			continue
		}
		pf := model.PersonFact{Fact: st.fact(h.name, "people.card", conf), Title: title}
		if scope != nil {
			st.contactsIn(scope, &pf)
			captured[scope] = true
		} else {
			// Several people share one container (flat lists, unclosed
			// cards): this person owns the siblings up to the next name.
			for sib := h.node.NextSibling; sib != nil; sib = sib.NextSibling {
				if isHit[sib] || len(names[sib]) > 0 {
					break
				}
				st.contactsIn(sib, &pf)
			}
		}
		st.people.add(pf)
	}

	if !peoplePage {
		return
	}
	// 3. Table rows: name in one cell, title in another.
	for _, tr := range rows {
		var cells []string
		for c := tr.FirstChild; c != nil; c = c.NextSibling {
			if c.DataAtom == atom.Td || c.DataAtom == atom.Th {
				cells = append(cells, collapse(textOf(c)))
			}
		}
		if len(cells) < 2 {
			continue
		}
		name := normalize.PersonName(cells[0])
		if !plausibleName(name, false) {
			continue
		}
		for _, c := range cells[1:] {
			if LooksLikeTitle(c) {
				pf := model.PersonFact{Fact: st.fact(name, "people.table", 0.7), Title: normalize.Title(c)}
				st.contactsIn(tr, &pf)
				st.people.add(pf)
				break
			}
		}
	}
	// 4. Single-line "Name – Title" entries.
	for _, n := range lineNodes {
		if insideCaptured(n, captured) {
			continue
		}
		txt := collapse(textOf(n))
		if len(txt) > 180 {
			continue
		}
		m := nameTitleLine.FindStringSubmatch(txt)
		if m == nil {
			continue
		}
		name := normalize.PersonName(m[1])
		if !plausibleName(name, false) || !LooksLikeTitle(m[2]) {
			continue
		}
		title := st.ownTitle(m[2])
		if title == "" {
			continue
		}
		pf := model.PersonFact{Fact: st.fact(name, "people.line", 0.6), Title: title}
		st.contactsIn(n, &pf)
		st.people.add(pf)
	}
}

func insideCaptured(n *html.Node, captured map[*html.Node]bool) bool {
	for a := n; a != nil; a = a.Parent {
		if captured[a] {
			return true
		}
	}
	return false
}

// findTitle looks for a job title inside a single-person container: first an
// element whose class says so, then the first title-like line after the name.
func findTitle(scope *html.Node, h nameHit) string {
	var explicit string
	var rec func(n *html.Node)
	rec = func(n *html.Node) {
		if explicit != "" || n.Type != html.ElementNode || n == h.node {
			return
		}
		if attr(n, "itemprop") == "jobTitle" || containsAny(strings.ToLower(attr(n, "class")), titleClassWords) {
			if t := collapse(textOf(n)); t != "" && len(t) <= 120 && !strings.EqualFold(t, h.name) && !strings.Contains(t, h.name) {
				explicit = t
				return
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			rec(c)
		}
	}
	for c := scope.FirstChild; c != nil; c = c.NextSibling {
		rec(c)
	}
	if explicit != "" {
		return explicit
	}
	return titleAfterLine(lines(textOf(scope)), h.name)
}

// titleAfterLine returns the first title-like line within two lines after
// the line holding name, or a title that shares the name's line.
func titleAfterLine(ls []string, name string) string {
	for i, l := range ls {
		if !strings.Contains(normalize.PersonName(l), name) && !strings.Contains(l, name) {
			continue
		}
		if rest := strings.Trim(strings.Replace(l, name, "", 1), " ,|–—-:•·"); rest != "" && LooksLikeTitle(rest) {
			return rest
		}
		for j := i + 1; j < len(ls) && j <= i+2; j++ {
			if LooksLikeTitle(ls[j]) {
				return ls[j]
			}
		}
		return ""
	}
	return ""
}

var atCompany = regexp.MustCompile(`(?i)^(.*?)(?:\s+at\s+|\s*@\s*)(.+)$`)

// ownTitle drops titles that place the person at another organisation
// ("CEO at CustomerCo"), which is how testimonials read. A trailing mention
// of this site's own brand is stripped instead.
func (st *state) ownTitle(title string) string {
	title = normalize.Title(title)
	if title == "" {
		return ""
	}
	if m := atCompany.FindStringSubmatch(title); m != nil {
		brand := st.brand
		if brand != "" && strings.Contains(normalize.Fold(m[2]), brand) {
			return normalize.Title(m[1])
		}
		return ""
	}
	return title
}

// contactsIn attaches the email, phone and social links found inside scope.
func (st *state) contactsIn(scope *html.Node, pf *model.PersonFact) {
	var rec func(n *html.Node)
	rec = func(n *html.Node) {
		if n.Type == html.ElementNode {
			if cf := attr(n, "data-cfemail"); cf != "" {
				if a := st.emails.add(DecodeCFEmail(cf), "cfemail", 0.9); a != "" {
					pf.Emails = mergeStrings(pf.Emails, []string{a})
				}
			}
			if n.DataAtom == atom.A {
				href := strings.TrimSpace(attr(n, "href"))
				switch {
				case hasPrefixFold(href, "mailto:"):
					for _, raw := range mailtoAddrs(href) {
						if a := st.emails.add(raw, "mailto", 0.95); a != "" {
							pf.Emails = mergeStrings(pf.Emails, []string{a})
						}
					}
				case strings.Contains(href, cfPrefix):
					i := strings.Index(href, cfPrefix)
					if a := st.emails.add(DecodeCFEmail(href[i+len(cfPrefix):]), "cfemail", 0.9); a != "" {
						pf.Emails = mergeStrings(pf.Emails, []string{a})
					}
				case hasPrefixFold(href, "tel:"):
					if p := st.phones.add(telNumber(href), "tel", 0.9, false); p != "" {
						pf.Phones = mergeStrings(pf.Phones, []string{p})
					}
				case maybeSocial(href):
					if pr, ok := CanonicalSocial(href); ok && pr.Scope != "company" {
						f := st.fact(pr.URL, "people.card", 0.85)
						pf.Socials = mergeSocials(pf.Socials, []model.SocialFact{{Fact: f, Network: pr.Network, Handle: pr.Handle, Scope: "person"}})
						st.personSocial[pr.URL] = true
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			rec(c)
		}
	}
	rec(scope)
	// Addresses written out in the card's text.
	sub := &emailSink{opt: st.emails.opt, best: map[string]model.EmailFact{}, now: st.emails.now}
	textEmails(collapse(textOf(scope)), sub)
	for _, a := range sub.list {
		ef := sub.best[a]
		if got := st.emails.add(a, ef.Extractor, ef.Confidence); got != "" {
			pf.Emails = mergeStrings(pf.Emails, []string{got})
		}
	}
}

// firstLine returns the first non-empty line of block text. Unclosed tags
// make later blocks nest inside a heading; only its own line is the name.
func firstLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		if l = collapse(l); l != "" {
			return l
		}
	}
	return ""
}

// titleInSiblings handles flat lists (<h4>Name</h4><p>Title</p>...): the
// title is the first title-like line in the next few siblings, stopping at
// the next person.
func titleInSiblings(n *html.Node, isHit map[*html.Node]bool, names map[*html.Node]map[string]struct{}) string {
	if own := lines(textOf(n)); len(own) > 1 && LooksLikeTitle(own[1]) {
		return own[1] // unclosed heading swallowed the title line
	}
	seen := 0
	for sib := n.NextSibling; sib != nil && seen < 3; sib = sib.NextSibling {
		if isHit[sib] || len(names[sib]) > 0 {
			break
		}
		var txt string
		switch sib.Type {
		case html.TextNode:
			txt = sib.Data
		case html.ElementNode:
			if isHidden(sib.DataAtom) {
				continue
			}
			txt = textOf(sib)
		default:
			continue
		}
		ls := lines(txt)
		if len(ls) == 0 {
			continue
		}
		seen++
		if len(ls[0]) <= 120 && LooksLikeTitle(ls[0]) {
			return ls[0]
		}
	}
	return ""
}
