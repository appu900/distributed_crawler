package graph

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

// MemNode is a node in the in-memory graph.
type MemNode struct {
	Label     string
	Key       string
	Props     map[string]any
	Scalars   map[string]model.Scalar
	Facts     []string
	FirstSeen time.Time
	LastSeen  time.Time
}

// Value returns a merge-ruled property value, or nil.
func (n *MemNode) Value(field string) any {
	if n == nil {
		return nil
	}
	return n.Scalars[field].Value
}

// MemEdge is a relationship in the in-memory graph.
type MemEdge struct {
	Type, FromLabel, FromKey, ToLabel, ToKey string
	Props                                    map[string]any
	SourceURL                                string
	Sources                                  []string
	Confidence                               float64
	FirstSeen, LastSeen                      time.Time
}

// Memory is a Store with the same merge semantics as the Cypher the Neo4j
// sink runs. It exists so the pipeline (including the crash-recovery
// scenarios) can be exercised without Docker, and it documents the merge
// rules in plain Go.
type Memory struct {
	mu         sync.Mutex
	nodes      map[string]*MemNode
	edges      map[string]*MemEdge
	suppressed map[string]map[string]struct{}
	maxFacts   int
	// Applied counts Apply calls, FailNext makes the next n calls fail.
	Applied  int
	FailNext int
	FailErr  error
}

func NewMemory(maxFacts int) *Memory {
	if maxFacts <= 0 {
		maxFacts = 50
	}
	return &Memory{nodes: map[string]*MemNode{}, edges: map[string]*MemEdge{}, suppressed: map[string]map[string]struct{}{}, maxFacts: maxFacts}
}

func nodeKey(label, key string) string { return label + "\x00" + key }

func edgeKey(typ, fromLabel, fromKey, toLabel, toKey string) string {
	return typ + "\x00" + fromLabel + "\x00" + fromKey + "\x00" + toLabel + "\x00" + toKey
}

// better mirrors the Cypher update condition.
func better(in, cur model.Scalar, has bool) bool {
	if !has {
		return true
	}
	if in.Confidence != cur.Confidence {
		return in.Confidence > cur.Confidence
	}
	return !in.At.Before(cur.At)
}

func (m *Memory) Apply(_ context.Context, muts []*model.GraphMutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Applied++
	if m.FailNext > 0 {
		m.FailNext--
		return m.FailErr
	}
	for _, mut := range muts {
		for _, n := range mut.Nodes {
			spec, ok := model.LabelByName(n.Label)
			if !ok || n.Key == "" {
				continue
			}
			k := nodeKey(n.Label, n.Key)
			cur := m.nodes[k]
			if cur == nil {
				cur = &MemNode{Label: n.Label, Key: n.Key, Props: map[string]any{}, Scalars: map[string]model.Scalar{},
					FirstSeen: n.SeenAt, LastSeen: n.SeenAt}
				m.nodes[k] = cur
			}
			if n.SeenAt.After(cur.LastSeen) {
				cur.LastSeen = n.SeenAt
			}
			if n.SeenAt.Before(cur.FirstSeen) {
				cur.FirstSeen = n.SeenAt
			}
			for pk, pv := range n.Props {
				if pv != nil {
					cur.Props[pk] = pv
				}
			}
			for _, f := range spec.Scalars {
				in, ok := n.Scalars[f.Name]
				if !ok || in.Value == nil {
					continue
				}
				in.Value = coerce(in.Value, f.Type)
				old, has := cur.Scalars[f.Name]
				if better(in, old, has) {
					cur.Scalars[f.Name] = in
				}
			}
			if spec.HasFacts {
				for _, f := range n.Facts {
					dup := false
					for _, have := range cur.Facts {
						if have == f {
							dup = true
							break
						}
					}
					if !dup {
						cur.Facts = append(cur.Facts, f)
					}
				}
				if len(cur.Facts) > m.maxFacts {
					cur.Facts = cur.Facts[len(cur.Facts)-m.maxFacts:]
				}
			}
		}
		for _, e := range mut.Edges {
			spec, ok := model.EdgeByShape(e.Type, e.FromLabel, e.ToLabel)
			if !ok {
				continue
			}
			if m.nodes[nodeKey(e.FromLabel, e.FromKey)] == nil || m.nodes[nodeKey(e.ToLabel, e.ToKey)] == nil {
				continue // MATCH found no endpoint
			}
			k := edgeKey(e.Type, e.FromLabel, e.FromKey, e.ToLabel, e.ToKey)
			cur := m.edges[k]
			if cur == nil {
				cur = &MemEdge{Type: e.Type, FromLabel: e.FromLabel, FromKey: e.FromKey, ToLabel: e.ToLabel, ToKey: e.ToKey,
					Props: map[string]any{}, Confidence: -1, FirstSeen: e.SeenAt, LastSeen: e.SeenAt}
				m.edges[k] = cur
			}
			win := e.Confidence >= cur.Confidence
			if e.SeenAt.After(cur.LastSeen) {
				cur.LastSeen = e.SeenAt
			}
			if e.SeenAt.Before(cur.FirstSeen) {
				cur.FirstSeen = e.SeenAt
			}
			has := false
			for _, s := range cur.Sources {
				if s == e.SourceURL {
					has = true
					break
				}
			}
			if !has && len(cur.Sources) < MaxEdgeSources {
				cur.Sources = append(cur.Sources, e.SourceURL)
			}
			if win {
				cur.SourceURL, cur.Confidence = e.SourceURL, e.Confidence
				for _, p := range spec.Props {
					if v, ok := e.Props[p]; ok && v != nil {
						cur.Props[p] = v
					}
				}
			}
		}
	}
	return nil
}

// Node returns a copy-free pointer to a node, or nil. Tests only.
func (m *Memory) Node(label, key string) *MemNode {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodes[nodeKey(label, key)]
}

// Edge returns a relationship, or nil. Tests only.
func (m *Memory) Edge(typ, fromLabel, fromKey, toLabel, toKey string) *MemEdge {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.edges[edgeKey(typ, fromLabel, fromKey, toLabel, toKey)]
}

// Nodes returns all nodes of a label, sorted by key.
func (m *Memory) Nodes(label string) []*MemNode {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*MemNode
	for _, n := range m.nodes {
		if n.Label == label {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Edges returns all relationships of a type, sorted.
func (m *Memory) Edges(typ string) []*MemEdge {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*MemEdge
	for _, e := range m.edges {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FromKey != out[j].FromKey {
			return out[i].FromKey < out[j].FromKey
		}
		return out[i].ToKey < out[j].ToKey
	})
	return out
}

func (m *Memory) Stats(context.Context) (Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Stats{Nodes: map[string]int64{}, Edges: map[string]int64{}}
	for _, n := range m.nodes {
		s.Nodes[n.Label]++
	}
	for _, e := range m.edges {
		s.Edges[e.Type]++
	}
	return s, nil
}

// deleteNode removes a node and every relationship touching it.
func (m *Memory) deleteNode(label, key string) bool {
	k := nodeKey(label, key)
	if m.nodes[k] == nil {
		return false
	}
	delete(m.nodes, k)
	for ek, e := range m.edges {
		if (e.FromLabel == label && e.FromKey == key) || (e.ToLabel == label && e.ToKey == key) {
			delete(m.edges, ek)
		}
	}
	return true
}

func (m *Memory) degree(label, key string) int {
	n := 0
	for _, e := range m.edges {
		if (e.FromLabel == label && e.FromKey == key) || (e.ToLabel == label && e.ToKey == key) {
			n++
		}
	}
	return n
}

// neighbours returns the (label,key) pairs reached from a node over the
// given relationship types, in either direction.
func (m *Memory) out(label, key string, types ...string) [][2]string {
	var out [][2]string
	for _, e := range m.edges {
		if e.FromLabel != label || e.FromKey != key {
			continue
		}
		for _, t := range types {
			if e.Type == t {
				out = append(out, [2]string{e.ToLabel, e.ToKey})
			}
		}
	}
	return out
}

func (m *Memory) in(label, key, typ, fromLabel string) []string {
	var out []string
	for _, e := range m.edges {
		if e.ToLabel == label && e.ToKey == key && e.Type == typ && e.FromLabel == fromLabel {
			out = append(out, e.FromKey)
		}
	}
	sort.Strings(out)
	return out
}

// redactPages collects and scrubs the pages that mention a node.
func (m *Memory) redactPages(label, key string) []PageRef {
	var refs []PageRef
	for _, url := range m.in(label, key, model.RelMentions, model.LabelPage) {
		pg := m.nodes[nodeKey(model.LabelPage, url)]
		if pg == nil {
			continue
		}
		ref := PageRef{URL: url}
		ref.RawKey, _ = pg.Value("raw_key").(string)
		ref.RawOffset, _ = pg.Value("raw_offset").(int64)
		ref.RawLength, _ = pg.Value("raw_length").(int64)
		ref.ExtractKey, _ = pg.Value("extract_key").(string)
		for _, f := range []string{"raw_key", "raw_offset", "raw_length", "extract_key", "content_hash"} {
			delete(pg.Scalars, f)
		}
		pg.Props["redacted"] = true
		refs = append(refs, ref)
	}
	return refs
}

func (m *Memory) erasePerson(personID string) EraseReport {
	var rep EraseReport
	p := m.nodes[nodeKey(model.LabelPerson, personID)]
	if p == nil {
		return rep
	}
	name, _ := p.Value("full_name").(string)
	companyID, _ := p.Props["company_id"].(string)
	rep.People = append(rep.People, ErasedPerson{ID: personID, CompanyID: companyID, Name: name})
	rep.Pages = m.redactPages(model.LabelPerson, personID)
	owned := m.out(model.LabelPerson, personID, model.RelHasEmail, model.RelHasPhone, model.RelHasSocial)
	m.deleteNode(model.LabelPerson, personID)
	rep.Nodes++
	for _, o := range owned {
		// Contact nodes that nobody else points at die with the person.
		others := 0
		for _, e := range m.edges {
			if e.ToLabel == o[0] && e.ToKey == o[1] && e.Type != model.RelMentions {
				others++
			}
		}
		if others == 0 {
			if o[0] == model.LabelEmail {
				rep.Emails = append(rep.Emails, o[1])
			}
			rep.Pages = append(rep.Pages, m.redactPages(o[0], o[1])...)
			if m.deleteNode(o[0], o[1]) {
				rep.Nodes++
			}
		}
	}
	return rep
}

func (m *Memory) ErasePerson(_ context.Context, personID string) (EraseReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.erasePerson(personID), nil
}

func (m *Memory) EraseEmail(_ context.Context, address string, cascade bool) (EraseReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rep EraseReport
	e := m.nodes[nodeKey(model.LabelEmail, address)]
	if e == nil {
		return rep, nil
	}
	owners := m.in(model.LabelEmail, address, model.RelHasEmail, model.LabelPerson)
	rep.Pages = m.redactPages(model.LabelEmail, address)
	personal := e.Props["kind"] == model.EmailPersonal
	m.deleteNode(model.LabelEmail, address)
	rep.Nodes++
	rep.Emails = append(rep.Emails, address)
	if cascade && personal {
		for _, id := range owners {
			rep.merge(m.erasePerson(id))
		}
	}
	return rep, nil
}

func (m *Memory) PurgeCompany(_ context.Context, companyID string) (EraseReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rep EraseReport
	if m.nodes[nodeKey(model.LabelCompany, companyID)] == nil {
		return rep, nil
	}
	for _, p := range m.out(model.LabelCompany, companyID, model.RelEmploys) {
		rep.merge(m.erasePerson(p[1]))
	}
	contacts := m.out(model.LabelCompany, companyID, model.RelHasEmail, model.RelHasPhone, model.RelHasSocial, model.RelLocatedAt)
	for _, d := range m.out(model.LabelCompany, companyID, model.RelHasDomain) {
		for _, pg := range m.out(model.LabelDomain, d[1], model.RelHasPage) {
			if n := m.nodes[nodeKey(model.LabelPage, pg[1])]; n != nil {
				ref := PageRef{URL: pg[1]}
				ref.RawKey, _ = n.Value("raw_key").(string)
				ref.RawOffset, _ = n.Value("raw_offset").(int64)
				ref.RawLength, _ = n.Value("raw_length").(int64)
				ref.ExtractKey, _ = n.Value("extract_key").(string)
				rep.Pages = append(rep.Pages, ref)
			}
			if m.deleteNode(model.LabelPage, pg[1]) {
				rep.Nodes++
			}
		}
		if m.deleteNode(model.LabelDomain, d[1]) {
			rep.Nodes++
		}
	}
	m.deleteNode(model.LabelCompany, companyID)
	rep.Nodes++
	for _, c := range contacts {
		if m.degree(c[0], c[1]) == 0 && m.deleteNode(c[0], c[1]) {
			rep.Nodes++
			if c[0] == model.LabelEmail {
				rep.Emails = append(rep.Emails, c[1])
			}
		}
	}
	return rep, nil
}

func (m *Memory) FlagSuppressed(_ context.Context, kind, member string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.suppressed[kind] == nil {
		m.suppressed[kind] = map[string]struct{}{}
	}
	m.suppressed[kind][member] = struct{}{}
	return nil
}

func (m *Memory) UnflagSuppressed(_ context.Context, kind, member string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.suppressed[kind], member)
	return nil
}

func (m *Memory) Suppressed(context.Context) (map[string][]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]string{}
	for kind, set := range m.suppressed {
		for member := range set {
			out[kind] = append(out[kind], member)
		}
		sort.Strings(out[kind])
	}
	return out, nil
}

func (m *Memory) Close(context.Context) error { return nil }
