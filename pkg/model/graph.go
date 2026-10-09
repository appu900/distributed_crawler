package model

import "time"

// Node labels.
const (
	LabelCompany    = "Company"
	LabelDomain     = "Domain"
	LabelPerson     = "Person"
	LabelEmail      = "Email"
	LabelPhone      = "Phone"
	LabelSocial     = "SocialProfile"
	LabelTechnology = "Technology"
	LabelLocation   = "Location"
	LabelPage       = "Page"
)

// Relationship types.
const (
	RelHasDomain = "HAS_DOMAIN"
	RelEmploys   = "EMPLOYS"
	RelHasEmail  = "HAS_EMAIL"
	RelHasPhone  = "HAS_PHONE"
	RelHasSocial = "HAS_SOCIAL"
	RelUses      = "USES"
	RelLocatedAt = "LOCATED_AT"
	RelHasPage   = "HAS_PAGE"
	RelMentions  = "MENTIONS"
	RelLinksTo   = "LINKS_TO"
)

// FieldType tells the writer how to coerce a scalar after JSON transport.
type FieldType int

const (
	FieldString FieldType = iota
	FieldInt
	FieldTime
)

// Field is a merge-ruled scalar property of a node.
type Field struct {
	Name string
	Type FieldType
}

// LabelSpec describes one node label.
type LabelSpec struct {
	Label string
	// Key is the identity property (unique constraint).
	Key string
	// Scalars are properties governed by the confidence/recency merge rule.
	Scalars []Field
	// HasFacts is set for labels that keep a `facts` provenance list.
	HasFacts bool
}

// Labels lists node labels in write order. Writing labels in one fixed order,
// with rows sorted by key, gives every transaction the same lock order.
var Labels = []LabelSpec{
	{Label: LabelCompany, Key: "id", HasFacts: true, Scalars: []Field{
		{Name: "name"}, {Name: "description"}, {Name: "industry"},
		{Name: "size_hint"}, {Name: "founded"}, {Name: "country"},
	}},
	{Label: LabelDomain, Key: "name"},
	{Label: LabelPerson, Key: "id", HasFacts: true, Scalars: []Field{
		{Name: "full_name"}, {Name: "title"}, {Name: "seniority"},
	}},
	{Label: LabelEmail, Key: "address"},
	{Label: LabelPhone, Key: "e164"},
	{Label: LabelSocial, Key: "url"},
	{Label: LabelTechnology, Key: "name"},
	{Label: LabelLocation, Key: "id"},
	{Label: LabelPage, Key: "url", Scalars: []Field{
		{Name: "type"}, {Name: "content_hash"}, {Name: "fetched_at", Type: FieldTime},
		{Name: "status", Type: FieldInt}, {Name: "skip_reason"}, {Name: "title"},
		{Name: "raw_key"}, {Name: "raw_offset", Type: FieldInt}, {Name: "raw_length", Type: FieldInt},
		{Name: "extract_key"},
	}},
}

// EdgeSpec describes one allowed (from)-[type]->(to) combination.
type EdgeSpec struct {
	Type, From, To string
	// Props are extra relationship properties copied verbatim.
	Props []string
}

// Edges lists relationship shapes in write order.
var Edges = []EdgeSpec{
	{Type: RelHasDomain, From: LabelCompany, To: LabelDomain},
	{Type: RelHasPage, From: LabelDomain, To: LabelPage},
	{Type: RelEmploys, From: LabelCompany, To: LabelPerson, Props: []string{"title"}},
	{Type: RelHasEmail, From: LabelCompany, To: LabelEmail},
	{Type: RelHasEmail, From: LabelPerson, To: LabelEmail},
	{Type: RelHasPhone, From: LabelCompany, To: LabelPhone},
	{Type: RelHasPhone, From: LabelPerson, To: LabelPhone},
	{Type: RelHasSocial, From: LabelCompany, To: LabelSocial},
	{Type: RelHasSocial, From: LabelPerson, To: LabelSocial},
	{Type: RelUses, From: LabelCompany, To: LabelTechnology},
	{Type: RelLocatedAt, From: LabelCompany, To: LabelLocation},
	{Type: RelMentions, From: LabelPage, To: LabelPerson},
	{Type: RelMentions, From: LabelPage, To: LabelEmail},
	{Type: RelMentions, From: LabelPage, To: LabelPhone},
	{Type: RelLinksTo, From: LabelCompany, To: LabelCompany},
}

// LabelByName returns the spec for a label.
func LabelByName(label string) (LabelSpec, bool) {
	for _, l := range Labels {
		if l.Label == label {
			return l, true
		}
	}
	return LabelSpec{}, false
}

// EdgeByShape returns the spec for a relationship shape.
func EdgeByShape(typ, from, to string) (EdgeSpec, bool) {
	for _, e := range Edges {
		if e.Type == typ && e.From == from && e.To == to {
			return e, true
		}
	}
	return EdgeSpec{}, false
}

// Scalar is one merge-ruled property value with its provenance weight.
type Scalar struct {
	Value      any       `json:"v"`
	Confidence float64   `json:"c"`
	At         time.Time `json:"t"`
}

// NodeUpsert is an idempotent node write.
type NodeUpsert struct {
	Label string `json:"label"`
	Key   string `json:"key"`
	// Props are identity-derived attributes; they never change for a key.
	Props map[string]any `json:"props,omitempty"`
	// Scalars follow the merge rule: higher confidence wins, newer wins ties.
	Scalars map[string]Scalar `json:"scalars,omitempty"`
	// Facts are serialized provenance records appended (set semantics).
	Facts  []string  `json:"facts,omitempty"`
	SeenAt time.Time `json:"seen_at"`
}

// EdgeUpsert is an idempotent relationship write.
type EdgeUpsert struct {
	Type       string         `json:"type"`
	FromLabel  string         `json:"from_label"`
	FromKey    string         `json:"from_key"`
	ToLabel    string         `json:"to_label"`
	ToKey      string         `json:"to_key"`
	Props      map[string]any `json:"props,omitempty"`
	SourceURL  string         `json:"source_url"`
	Confidence float64        `json:"confidence"`
	SeenAt     time.Time      `json:"seen_at"`
}

// GraphMutation is the resolver's output for one extraction.
type GraphMutation struct {
	SourceURL string       `json:"source_url"`
	Nodes     []NodeUpsert `json:"nodes,omitempty"`
	Edges     []EdgeUpsert `json:"edges,omitempty"`
	// PurgeCompanies lists company IDs whose whole subgraph must be removed
	// (country policy).
	PurgeCompanies []string `json:"purge_companies,omitempty"`
}

// Rows returns the number of node and edge rows in the mutation.
func (m *GraphMutation) Rows() int { return len(m.Nodes) + len(m.Edges) }
