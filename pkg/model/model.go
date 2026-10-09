// Package model holds the wire types passed between pipeline stages and the
// static description of the graph schema.
package model

import "time"

// Fact is the provenance envelope carried by every extracted value.
type Fact struct {
	Value       string    `json:"value"`
	SourceURL   string    `json:"source_url"`
	Extractor   string    `json:"extractor"`
	Confidence  float64   `json:"confidence"`
	ExtractedAt time.Time `json:"extracted_at"`
}

// PageType classifies a crawled page.
type PageType string

const (
	PageHome    PageType = "home"
	PageAbout   PageType = "about"
	PageTeam    PageType = "team"
	PageContact PageType = "contact"
	PageCareers PageType = "careers"
	PageOther   PageType = "other"
)

// Email kinds.
const (
	EmailRole     = "role"
	EmailPersonal = "personal"
)

// EmailFact is an email address; Value is the lowercased address.
type EmailFact struct {
	Fact
	Kind       string `json:"kind"`
	Domain     string `json:"domain"`
	Disposable bool   `json:"disposable,omitempty"`
}

// PhoneFact is a phone number; Value is E.164.
type PhoneFact struct {
	Fact
	Kind string `json:"kind"`
	Raw  string `json:"raw,omitempty"`
}

// SocialFact is a social profile; Value is the canonical profile URL.
type SocialFact struct {
	Fact
	Network string `json:"network"`
	Handle  string `json:"handle"`
	// Scope is "company" or "person".
	Scope string `json:"scope"`
}

// AddressFact is a postal address; Value is a single-line rendering.
type AddressFact struct {
	Fact
	Street  string `json:"street,omitempty"`
	City    string `json:"city,omitempty"`
	Region  string `json:"region,omitempty"`
	Postal  string `json:"postal,omitempty"`
	Country string `json:"country,omitempty"`
}

// PersonFact is a person found on a page; Value is the display name.
type PersonFact struct {
	Fact
	Title   string       `json:"title,omitempty"`
	Emails  []string     `json:"emails,omitempty"`
	Phones  []string     `json:"phones,omitempty"`
	Socials []SocialFact `json:"socials,omitempty"`
}

// TechFact is a detected technology; Value is its name.
type TechFact struct {
	Fact
	Category string `json:"category"`
}

// Link is an in-site link discovered on a page.
type Link struct {
	URL string `json:"url"`
}

// Extraction is everything the parser derived from one fetched page.
type Extraction struct {
	URL        string    `json:"url"`
	Host       string    `json:"host"`
	Domain     string    `json:"domain"`
	PageType   PageType  `json:"page_type"`
	Title      string    `json:"title,omitempty"`
	Lang       string    `json:"lang,omitempty"`
	Status     int       `json:"status"`
	SkipReason string    `json:"skip_reason,omitempty"`
	Depth      int       `json:"depth"`
	Probe      bool      `json:"probe,omitempty"`
	FetchedAt  time.Time `json:"fetched_at"`

	ContentHash string `json:"content_hash,omitempty"`
	RawKey      string `json:"raw_key,omitempty"`
	RawOffset   int64  `json:"raw_offset,omitempty"`
	RawLength   int64  `json:"raw_length,omitempty"`
	ExtractKey  string `json:"extract_key,omitempty"`

	NoIndex   bool `json:"noindex,omitempty"`
	NoFollow  bool `json:"nofollow,omitempty"`
	Unchanged bool `json:"unchanged,omitempty"`

	CompanyNames []Fact        `json:"company_names,omitempty"`
	Descriptions []Fact        `json:"descriptions,omitempty"`
	Industries   []Fact        `json:"industries,omitempty"`
	SizeHints    []Fact        `json:"size_hints,omitempty"`
	Founded      []Fact        `json:"founded,omitempty"`
	Emails       []EmailFact   `json:"emails,omitempty"`
	Phones       []PhoneFact   `json:"phones,omitempty"`
	Socials      []SocialFact  `json:"socials,omitempty"`
	Addresses    []AddressFact `json:"addresses,omitempty"`
	People       []PersonFact  `json:"people,omitempty"`
	Technologies []TechFact    `json:"technologies,omitempty"`

	Links           []Link   `json:"links,omitempty"`
	OutboundDomains []string `json:"outbound_domains,omitempty"`
}

// Fetch request kinds.
const (
	KindPage    = "p"
	KindSitemap = "s"
)

// FetchRequest is one URL handed from the frontier to the fetcher.
type FetchRequest struct {
	URL     string `json:"url"`
	Kind    string `json:"kind"`
	Depth   int    `json:"depth"`
	Attempt int    `json:"attempt"`
	// Hops counts the redirects followed to reach this URL.
	Hops   int    `json:"hops,omitempty"`
	Seed   bool   `json:"seed,omitempty"`
	Probe  bool   `json:"probe,omitempty"`
	Domain string `json:"domain"`
}

// Skip reasons recorded when a URL is not fetched or not stored.
const (
	SkipRobots      = "robots_disallowed"
	SkipContentType = "content_type"
	SkipOffDomain   = "redirect_off_domain"
	SkipTooManyHops = "too_many_redirects"
	SkipFetchFailed = "fetch_failed"
	SkipCircuitOpen = "circuit_open"
	SkipSuppressed  = "suppressed"
	SkipBlocked     = "blocked_private_address"
)

// FetchResult is published by the fetcher for every attempted URL.
type FetchResult struct {
	URL         string    `json:"url"`
	FinalURL    string    `json:"final_url,omitempty"`
	Kind        string    `json:"kind"`
	Depth       int       `json:"depth"`
	Probe       bool      `json:"probe,omitempty"`
	Status      int       `json:"status"`
	S3Key       string    `json:"s3_key,omitempty"`
	RawOffset   int64     `json:"raw_offset,omitempty"`
	RawLength   int64     `json:"raw_length,omitempty"`
	FetchedAt   time.Time `json:"fetched_at"`
	ContentHash string    `json:"content_hash,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
	SkipReason  string    `json:"skip_reason,omitempty"`
	Rendered    bool      `json:"rendered,omitempty"`
}

// RawRecord is one line of a raw batch object.
type RawRecord struct {
	URL         string              `json:"url"`
	FinalURL    string              `json:"final_url,omitempty"`
	Kind        string              `json:"kind,omitempty"`
	Status      int                 `json:"status"`
	Headers     map[string][]string `json:"headers,omitempty"`
	FetchedAt   time.Time           `json:"fetched_at"`
	ContentHash string              `json:"content_hash,omitempty"`
	Truncated   bool                `json:"truncated,omitempty"`
	Rendered    bool                `json:"rendered,omitempty"`
	Body        []byte              `json:"body,omitempty"`
	Tombstone   bool                `json:"tombstone,omitempty"`
}
