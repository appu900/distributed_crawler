package resolve

import (
	"strings"

	"github.com/nyaruka/phonenumbers"

	"github.com/appu/leadgraph/internal/config"
)

// Policy is the country allow/deny list.
type Policy struct {
	allow        map[string]struct{}
	deny         map[string]struct{}
	allowUnknown bool
}

func NewPolicy(c config.Countries) Policy {
	p := Policy{allow: map[string]struct{}{}, deny: map[string]struct{}{}, allowUnknown: c.Unknown != "deny"}
	for _, cc := range c.Allow {
		p.allow[strings.ToUpper(cc)] = struct{}{}
	}
	for _, cc := range c.Deny {
		p.deny[strings.ToUpper(cc)] = struct{}{}
	}
	return p
}

// Active reports whether any list is configured.
func (p Policy) Active() bool { return len(p.allow) > 0 || len(p.deny) > 0 }

// Allowed applies the policy. The deny list always wins; with an allow list
// only listed countries pass; an unknown country follows the configured
// default when an allow list exists and passes otherwise.
func (p Policy) Allowed(iso string) bool {
	iso = strings.ToUpper(iso)
	if iso == "" {
		if len(p.allow) > 0 {
			return p.allowUnknown
		}
		return true
	}
	if _, denied := p.deny[iso]; denied {
		return false
	}
	if len(p.allow) > 0 {
		_, ok := p.allow[iso]
		return ok
	}
	return true
}

// phoneRegion returns the ISO region of an E.164 number, or "".
func phoneRegion(e164 string) string {
	n, err := phonenumbers.Parse(e164, "")
	if err != nil {
		return ""
	}
	r := phonenumbers.GetRegionCodeForNumber(n)
	if r == "ZZ" || r == "001" {
		return ""
	}
	return r
}
