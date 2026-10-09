package extract

import (
	"net/url"
	"strings"

	"github.com/appu/leadgraph/pkg/model"
)

// Social networks.
const (
	NetLinkedIn  = "linkedin"
	NetX         = "x"
	NetFacebook  = "facebook"
	NetInstagram = "instagram"
	NetGitHub    = "github"
	NetYouTube   = "youtube"
)

// Profile is a canonicalized social profile reference.
type Profile struct {
	URL, Network, Handle string
	// Scope is "person", "company" or "" when the URL does not say.
	Scope string
}

var reservedHandles = map[string]map[string]struct{}{
	NetX: toSet(`intent share home search hashtag i login signup explore settings privacy tos
		about compose messages notifications download jobs`),
	NetFacebook: toSet(`sharer sharer.php share.php share dialog plugins tr login groups events
		watch marketplace gaming help policies privacy legal business ads l.php hashtag photo.php
		story.php permalink.php login.php home.php people public`),
	NetInstagram: toSet(`p reel reels explore accounts stories tv about developer legal directory`),
	NetGitHub: toSet(`features pricing about login join sponsors marketplace topics apps settings
		site security enterprise readme explore collections trending contact customer-stories
		team solutions resources events search notifications issues pulls new organizations`),
}

// CanonicalSocial recognizes a profile URL on a supported network and
// returns its canonical form. Share, intent and content URLs are rejected.
func CanonicalSocial(raw string) (Profile, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return Profile{}, false
	}
	host := strings.ToLower(u.Hostname())
	for _, pre := range []string{"www.", "m.", "mobile.", "web.", "business."} {
		host = strings.TrimPrefix(host, pre)
	}
	segs := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	if len(segs) == 0 {
		return Profile{}, false
	}
	first := strings.ToLower(segs[0])
	switch {
	case host == "linkedin.com" || strings.HasSuffix(host, ".linkedin.com"):
		if len(segs) < 2 {
			return Profile{}, false
		}
		slug := strings.ToLower(segs[1])
		if un, err := url.PathUnescape(slug); err == nil {
			slug = strings.ToLower(un)
		}
		switch first {
		case "in", "pub":
			return Profile{"https://www.linkedin.com/in/" + url.PathEscape(slug), NetLinkedIn, slug, "person"}, true
		case "company", "school", "showcase":
			return Profile{"https://www.linkedin.com/" + first + "/" + url.PathEscape(slug), NetLinkedIn, slug, "company"}, true
		}
	case host == "twitter.com" || host == "x.com":
		h := strings.TrimPrefix(first, "@")
		if validHandle(h, 15, "_") && !reserved(NetX, h) {
			return Profile{"https://x.com/" + h, NetX, h, ""}, true
		}
	case host == "facebook.com" || host == "fb.com":
		if first == "pages" && len(segs) >= 3 {
			return Profile{"https://www.facebook.com/" + strings.ToLower(segs[len(segs)-1]), NetFacebook, strings.ToLower(segs[1]), ""}, true
		}
		if first == "profile.php" {
			if id := u.Query().Get("id"); id != "" && validHandle(id, 30, "") {
				return Profile{"https://www.facebook.com/profile.php?id=" + id, NetFacebook, id, "person"}, true
			}
			return Profile{}, false
		}
		if validHandle(first, 80, ".-") && !reserved(NetFacebook, first) {
			return Profile{"https://www.facebook.com/" + first, NetFacebook, first, ""}, true
		}
	case host == "instagram.com":
		if validHandle(first, 30, "._") && !reserved(NetInstagram, first) {
			return Profile{"https://www.instagram.com/" + first, NetInstagram, first, ""}, true
		}
	case host == "github.com":
		if first == "orgs" && len(segs) >= 2 {
			first = strings.ToLower(segs[1])
		}
		if validHandle(first, 39, "-") && !reserved(NetGitHub, first) {
			return Profile{"https://github.com/" + first, NetGitHub, first, ""}, true
		}
	case host == "youtube.com":
		switch {
		case strings.HasPrefix(first, "@") && len(first) > 1:
			return Profile{"https://www.youtube.com/" + first, NetYouTube, first[1:], ""}, true
		case (first == "channel" || first == "c" || first == "user") && len(segs) >= 2:
			id := segs[1]
			if first != "channel" {
				id = strings.ToLower(id)
			}
			return Profile{"https://www.youtube.com/" + first + "/" + id, NetYouTube, id, ""}, true
		}
	}
	return Profile{}, false
}

func reserved(network, handle string) bool {
	_, ok := reservedHandles[network][handle]
	return ok
}

func validHandle(h string, maxLen int, extra string) bool {
	if h == "" || len(h) > maxLen {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte(extra, c) >= 0) {
			return false
		}
	}
	return true
}

type socialSink struct {
	best map[string]model.SocialFact
	list []string
	now  func() model.Fact
}

func (s *socialSink) add(raw, extractor string, conf float64, defaultScope string) (model.SocialFact, bool) {
	pr, ok := CanonicalSocial(raw)
	if !ok {
		return model.SocialFact{}, false
	}
	scope := pr.Scope
	if scope == "" {
		scope = defaultScope
	}
	f := s.now()
	f.Value, f.Extractor, f.Confidence = pr.URL, extractor, conf
	sf := model.SocialFact{Fact: f, Network: pr.Network, Handle: pr.Handle, Scope: scope}
	if prev, seen := s.best[pr.URL]; seen {
		if prev.Confidence >= conf {
			return prev, true
		}
	} else {
		s.list = append(s.list, pr.URL)
	}
	s.best[pr.URL] = sf
	return sf, true
}

func (s *socialSink) facts() []model.SocialFact {
	out := make([]model.SocialFact, 0, len(s.list))
	for _, k := range s.list {
		out = append(out, s.best[k])
	}
	return out
}

// socialHosts is a cheap prefilter so only candidate hrefs are URL-parsed.
var socialHosts = []string{"linkedin.com", "twitter.com", "x.com", "facebook.com", "fb.com", "instagram.com", "github.com", "youtube.com"}

func maybeSocial(href string) bool {
	for _, h := range socialHosts {
		if strings.Contains(href, h) {
			return true
		}
	}
	return false
}
