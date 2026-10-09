package extract

import (
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/publicsuffix"

	"github.com/appu/leadgraph/pkg/model"
)

// roleLocals are mailbox names that address a function, not a person.
var roleLocals = toSet(`info sales support contact hello hi admin office team hr jobs job careers career press
media marketing billing accounts accounting finance legal privacy security abuse postmaster webmaster
hostmaster noreply no-reply donotreply do-not-reply help helpdesk service services customerservice
customercare customer care enquiries enquiry inquiries inquiry mail email post kontakt hallo bewerbung
partners partnerships partner investors ir pr feedback orders order booking bookings reservations
reception newsletter news dpo gdpr compliance dev developers api it ops recruiting recruitment talent
general welcome community events legal invoices invoice payments purchasing procurement shop store
returns success solutions business bizdev training academy education datenschutz buchhaltung vertrieb
verwaltung zentrale empfang presse karriere contacto ventas contato vendas commercial comptabilite
all everyone staff management board ask connect web website digital tech technical quality data`)

// disposableDomains is a built-in list of throwaway mailbox providers.
var disposableDomains = toSet(`mailinator.com guerrillamail.com guerrillamail.net guerrillamail.org
10minutemail.com 10minutemail.net tempmail.com temp-mail.org tempmail.net yopmail.com yopmail.fr
trashmail.com trashmail.de sharklasers.com getnada.com nada.email throwawaymail.com maildrop.cc
dispostable.com fakeinbox.com mailnesia.com mintemail.com mytemp.email moakt.com emailondeck.com
tempr.email discard.email mailcatch.com spamgourmet.com mohmal.com tempinbox.com burnermail.io
getairmail.com inboxkitten.com mail-temporaire.fr wegwerfmail.de einrot.com grr.la spam4.me`)

var placeholderDomains = toSet(`example.com example.org example.net domain.com yourdomain.com
email.com mail.com company.com yourcompany.com mysite.com website.com sentry.io sentry-next.wixpress.com
sentry.wixpress.com test.com mydomain.com site.com address.com`)

var placeholderLocals = toSet(`you your yourname youremail name email user username example
firstname.lastname first.last firstname lastname someone somebody xxx`)

var assetExts = toSet(`png jpg jpeg gif svg webp css js json xml pdf zip mov mp4 webm woff woff2 ttf
eot ico avif bmp tiff map scss`)

func toSet(s string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, f := range strings.Fields(s) {
		m[f] = struct{}{}
	}
	return m
}

// EmailOptions tunes address validation.
type EmailOptions struct {
	// AllowReservedTLDs accepts .test/.example/.localhost/.invalid domains,
	// which only exist in fixtures.
	AllowReservedTLDs bool
}

var reservedTLDs = toSet("test example localhost invalid local")

// CleanEmail lowercases and validates an address, rejecting placeholders,
// asset filenames and unknown TLDs. It returns "" when the input is not a
// plausible deliverable address.
func CleanEmail(raw string, opt EmailOptions) string {
	e := strings.ToLower(strings.TrimSpace(raw))
	e = strings.Trim(e, ".,;:<>()[]\"'")
	at := strings.LastIndexByte(e, '@')
	if at <= 0 || at == len(e)-1 || len(e) > 254 {
		return ""
	}
	local, domain := e[:at], e[at+1:]
	if len(local) > 64 || strings.HasPrefix(local, ".") || strings.HasSuffix(local, ".") || strings.Contains(local, "..") {
		return ""
	}
	for i := 0; i < len(local); i++ {
		if !isLocalByte(local[i]) {
			return ""
		}
	}
	domain = strings.TrimSuffix(domain, ".")
	labels := strings.Split(domain, ".")
	if len(labels) < 2 {
		return ""
	}
	for _, l := range labels {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return ""
		}
		for i := 0; i < len(l); i++ {
			if !isDomainByte(l[i]) {
				return ""
			}
		}
	}
	tld := labels[len(labels)-1]
	if _, bad := assetExts[tld]; bad {
		return ""
	}
	for i := 0; i < len(tld); i++ {
		if tld[i] >= '0' && tld[i] <= '9' {
			return ""
		}
	}
	if _, reserved := reservedTLDs[tld]; reserved {
		if !opt.AllowReservedTLDs {
			return ""
		}
	} else if _, icann := publicsuffix.PublicSuffix("x." + tld); !icann {
		return ""
	}
	if _, ph := placeholderDomains[domain]; ph {
		return ""
	}
	if _, ph := placeholderLocals[local]; ph {
		return ""
	}
	if isHexToken(local) {
		// Error-tracker DSNs and hashed IDs, not mailboxes.
		return ""
	}
	return local + "@" + domain
}

func isLocalByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '%' || c == '+' || c == '-'
}

func isDomainByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-'
}

func isHexToken(s string) bool {
	if len(s) < 24 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ClassifyEmail reports whether an address is a role mailbox or a personal one.
func ClassifyEmail(addr string) string {
	local := addr
	if at := strings.IndexByte(addr, '@'); at >= 0 {
		local = addr[:at]
	}
	if plus := strings.IndexByte(local, '+'); plus > 0 {
		local = local[:plus]
	}
	if _, ok := roleLocals[local]; ok {
		return model.EmailRole
	}
	parts := strings.FieldsFunc(local, func(r rune) bool { return r == '.' || r == '-' || r == '_' })
	if len(parts) == 0 {
		return model.EmailRole
	}
	for _, p := range parts {
		p = strings.TrimRight(p, "0123456789")
		if _, ok := roleLocals[p]; !ok {
			return model.EmailPersonal
		}
	}
	return model.EmailRole
}

// IsDisposable reports whether a domain is a known throwaway provider.
func IsDisposable(domain string) bool {
	_, ok := disposableDomains[strings.ToLower(domain)]
	return ok
}

// DecodeCFEmail decodes Cloudflare's email obfuscation: the first byte is an
// XOR key for the rest.
func DecodeCFEmail(encoded string) string {
	b, err := hex.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(b) < 2 {
		return ""
	}
	key := b[0]
	out := make([]byte, len(b)-1)
	for i, c := range b[1:] {
		out[i] = c ^ key
	}
	return string(out)
}

// maxObfuscationWindows bounds de-obfuscation work on hostile pages.
const maxObfuscationWindows = 400

const cfPrefix = "/cdn-cgi/l/email-protection#"

// obfuscated matches "local [at] domain [dot] tld" style spellings inside a
// small window around an "at" marker.
var obfuscated = regexp.MustCompile(`(?i)([a-z0-9][a-z0-9._%+-]{0,63})\s*(` +
	`[\[({<]\s*(?:at|@)\s*[\])}>]|\s@\s|\sat\s` +
	// A plain "." joins labels only without whitespace, so a sentence end
	// ("acme [dot] com. Next sentence") is not read as part of the domain.
	`)\s*((?:[a-z0-9-]{1,63}(?:\s*[\[({<]\s*(?:dot|\.)\s*[\])}>]\s*|\sdot\s|\.)){1,4}[a-z]{2,24})\b`)

var dotMarker = regexp.MustCompile(`(?i)\s*(?:[\[({<]\s*(?:dot|\.)\s*[\])}>]|\sdot\s)\s*`)

type emailSink struct {
	opt  EmailOptions
	best map[string]model.EmailFact
	list []string
	src  string
	now  func() model.Fact
}

func (s *emailSink) add(raw, extractor string, conf float64) string {
	addr := CleanEmail(raw, s.opt)
	if addr == "" {
		return ""
	}
	if prev, ok := s.best[addr]; ok {
		if prev.Confidence >= conf {
			return addr
		}
	} else {
		s.list = append(s.list, addr)
	}
	f := s.now()
	f.Value, f.Extractor, f.Confidence = addr, extractor, conf
	domain := addr[strings.IndexByte(addr, '@')+1:]
	s.best[addr] = model.EmailFact{Fact: f, Kind: ClassifyEmail(addr), Domain: domain, Disposable: IsDisposable(domain)}
	return addr
}

func (s *emailSink) facts() []model.EmailFact {
	out := make([]model.EmailFact, 0, len(s.list))
	for _, a := range s.list {
		out = append(out, s.best[a])
	}
	return out
}

// mailtoAddrs returns the addresses in a mailto: href.
func mailtoAddrs(href string) []string {
	rest := href[len("mailto:"):]
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rest = rest[:i]
	}
	if un, err := url.PathUnescape(rest); err == nil {
		rest = un
	}
	return strings.FieldsFunc(rest, func(r rune) bool { return r == ',' || r == ';' || r == ' ' })
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func (p *page) extractEmails(s *emailSink) {
	for _, a := range p.anchors {
		switch {
		case hasPrefixFold(a.href, "mailto:"):
			for _, addr := range mailtoAddrs(a.href) {
				s.add(addr, "mailto", 0.95)
			}
		case strings.Contains(a.href, cfPrefix):
			i := strings.Index(a.href, cfPrefix)
			s.add(DecodeCFEmail(a.href[i+len(cfPrefix):]), "cfemail", 0.9)
		}
	}
	for _, cf := range p.cfemails {
		s.add(DecodeCFEmail(cf), "cfemail", 0.9)
	}
	textEmails(p.text, s)
}

// textEmails finds plain and obfuscated addresses in visible text.
func textEmails(text string, s *emailSink) {
	// Plain addresses: expand outwards from every '@'.
	for i := 0; i < len(text); i++ {
		if text[i] != '@' {
			continue
		}
		l := i
		for l > 0 && isLocalByte(lower(text[l-1])) {
			l--
		}
		r := i + 1
		for r < len(text) && (isDomainByte(lower(text[r])) || text[r] == '.') {
			r++
		}
		if l < i && r > i+1 {
			s.add(text[l:r], "text", 0.8)
		}
	}
	// Obfuscated addresses: look only near "at"/"@" markers so the regexp
	// never scans the whole document.
	lastEnd, skipTo, windows := 0, 0, 0
	for i := 0; i < len(text) && windows < maxObfuscationWindows; i++ {
		c := text[i]
		var marker bool
		switch {
		case c == '@':
			marker = i > 0 && i+1 < len(text) && (isOpen(text[i-1]) || text[i-1] == ' ') && (isClose(text[i+1]) || text[i+1] == ' ')
		case (c == 'a' || c == 'A') && i+2 < len(text) && (text[i+1] == 't' || text[i+1] == 'T') && i > 0:
			prev, next := text[i-1], text[i+2]
			marker = (isOpen(prev) || prev == ' ') && (isClose(next) || next == ' ')
		}
		if !marker || i < skipTo {
			continue
		}
		windows++
		start := max(lastEnd, i-80)
		end := min(len(text), i+160)
		// Markers that the regexp already saw with full context need no
		// window of their own.
		skipTo = end - 100
		for start > 0 && start < i && text[start]&0xC0 == 0x80 {
			start++ // do not cut a UTF-8 sequence
		}
		window := text[start:end]
		for _, m := range obfuscated.FindAllStringSubmatchIndex(window, -1) {
			local, at, dom := window[m[2]:m[3]], window[m[4]:m[5]], window[m[6]:m[7]]
			bareAt := !strings.ContainsAny(at, "[({<@")
			spelledDot := dotMarker.MatchString(dom)
			if bareAt && !spelledDot {
				// "visit us at example.com" is prose, not an address.
				continue
			}
			domain := strings.ReplaceAll(dotMarker.ReplaceAllString(dom, "."), " ", "")
			conf := 0.75
			if bareAt {
				conf = 0.5
			}
			// The domain match is greedy; if it ran into following words,
			// drop trailing labels until the address validates.
			for {
				if s.add(local+"@"+domain, "deobfuscate", conf) != "" {
					lastEnd = start + m[1]
					break
				}
				cut := strings.LastIndexByte(domain, '.')
				if cut < 0 || !strings.Contains(domain[:cut], ".") {
					break
				}
				domain = domain[:cut]
			}
		}
		if i < lastEnd {
			i = lastEnd
		}
	}
}

func lower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

func isOpen(c byte) bool  { return c == '[' || c == '(' || c == '{' || c == '<' }
func isClose(c byte) bool { return c == ']' || c == ')' || c == '}' || c == '>' }
