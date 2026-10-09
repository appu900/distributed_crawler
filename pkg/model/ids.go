package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func hashID(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// CompanyID is the deterministic ID of the company owning a registrable domain.
func CompanyID(registrableDomain string) string {
	return hashID("company", strings.ToLower(registrableDomain))
}

// PersonID is the deterministic ID of a person at a company. nameKey is the
// folded name; discriminator is the LinkedIn profile URL when known, else
// the folded job title (possibly empty).
func PersonID(companyID, nameKey, discriminator string) string {
	return hashID("person", companyID, nameKey, discriminator)
}

// EmailID is the hash of the lower-cased address. It is what suppression
// lists store, so they never hold the address itself.
func EmailID(address string) string {
	return hashID("email", strings.ToLower(strings.TrimSpace(address)))
}

// PersonNameKey identifies "this name at this company" independent of title.
func PersonNameKey(companyID, nameKey string) string {
	return hashID("person-name", companyID, nameKey)
}

// LocationID is the deterministic ID of a postal location.
func LocationID(country, region, city, postal, street string) string {
	return hashID("location", strings.ToLower(country), strings.ToLower(region), strings.ToLower(city),
		strings.ToLower(strings.ReplaceAll(postal, " ", "")), strings.ToLower(street))
}
