// Package graph writes mutations to the property graph. It contains the
// Cypher generation, the Neo4j sink, an in-memory sink with identical merge
// semantics, the migration runner, the writer stage and erasure.
package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

// NodeCypher returns the idempotent upsert statement for a label.
//
// Each row is {key, first, last, props, s: {field: {v, c, t}}, facts}, and a
// key appears at most once per statement (see mergeNodes). A scalar is
// replaced only when the incoming confidence is strictly higher, or equal
// with a timestamp that is not older. The decision is computed in a WITH
// before any SET so value, confidence and timestamp always move together.
func NodeCypher(spec model.LabelSpec) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "UNWIND $rows AS row\nMERGE (n:%s {%s: row.key})\n", spec.Label, spec.Key)
	sb.WriteString("ON CREATE SET n.first_seen = row.first, n.last_seen = row.last\n")
	sb.WriteString("SET n += row.props\n")
	sb.WriteString("WITH n, row")
	for _, f := range spec.Scalars {
		fmt.Fprintf(&sb, ",\n  (row.s.%[1]s IS NOT NULL AND (n.%[1]s_conf IS NULL OR row.s.%[1]s.c > n.%[1]s_conf OR "+
			"(row.s.%[1]s.c = n.%[1]s_conf AND row.s.%[1]s.t >= n.%[1]s_at))) AS u_%[1]s", f.Name)
	}
	sb.WriteString("\nSET n.last_seen = CASE WHEN row.last > n.last_seen THEN row.last ELSE n.last_seen END,\n")
	sb.WriteString("    n.first_seen = CASE WHEN row.first < n.first_seen THEN row.first ELSE n.first_seen END")
	for _, f := range spec.Scalars {
		fmt.Fprintf(&sb, ",\n    n.%[1]s = CASE WHEN u_%[1]s THEN row.s.%[1]s.v ELSE n.%[1]s END"+
			",\n    n.%[1]s_conf = CASE WHEN u_%[1]s THEN row.s.%[1]s.c ELSE n.%[1]s_conf END"+
			",\n    n.%[1]s_at = CASE WHEN u_%[1]s THEN row.s.%[1]s.t ELSE n.%[1]s_at END", f.Name)
	}
	if spec.HasFacts {
		sb.WriteString("\nWITH n, row, [f IN row.facts WHERE NOT f IN coalesce(n.facts, [])] AS nf\n")
		sb.WriteString("SET n.facts = (coalesce(n.facts, []) + nf)[-$maxFacts..]")
	}
	return sb.String()
}

// EdgeCypher returns the idempotent upsert statement for a relationship
// shape. Both endpoints are MATCHed, so an edge to a node that was filtered
// out (suppressed) or does not exist yet is simply not created.
func EdgeCypher(spec model.EdgeSpec) string {
	from, _ := model.LabelByName(spec.From)
	to, _ := model.LabelByName(spec.To)
	return fmt.Sprintf(`UNWIND $rows AS row
MATCH (a:%s {%s: row.from})
MATCH (b:%s {%s: row.to})
MERGE (a)-[r:%s]->(b)
ON CREATE SET r.first_seen = row.first, r.last_seen = row.last, r.confidence = -1.0, r.sources = []
WITH r, row, row.confidence >= r.confidence AS win
SET r.last_seen = CASE WHEN row.last > r.last_seen THEN row.last ELSE r.last_seen END,
    r.first_seen = CASE WHEN row.first < r.first_seen THEN row.first ELSE r.first_seen END,
    r.source_url = CASE WHEN win THEN row.source_url ELSE r.source_url END,
    r.sources = (r.sources + [s IN row.sources WHERE NOT s IN r.sources])[..$maxSources],
    r.confidence = CASE WHEN win THEN row.confidence ELSE r.confidence END
FOREACH (_ IN CASE WHEN win THEN [1] ELSE [] END | SET r += row.props)`,
		spec.From, from.Key, spec.To, to.Key, spec.Type)
}

// MaxEdgeSources bounds the provenance list kept on a relationship.
const MaxEdgeSources = 20

// Statement is one UNWIND with its rows.
type Statement struct {
	Cypher string
	Rows   []map[string]any
	Kind   string // "node" or "edge"
	Name   string // label or relationship shape, for logs and metrics
}

// coerce restores a scalar's type after JSON transport.
func coerce(v any, t model.FieldType) any {
	switch t {
	case model.FieldInt:
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int:
			return int64(n)
		}
	case model.FieldTime:
		if s, ok := v.(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return ts.UTC()
			}
		}
	}
	return v
}

type mergedNode struct {
	model.NodeUpsert
	first, last time.Time
}

// mergeNodes folds upserts of the same key into one, applying the merge rule
// in Go. This is required for correctness, not just economy: when one UNWIND
// touches a node twice, Neo4j evaluates every row's update condition against
// the node as it was before the statement, so the last row would win
// regardless of confidence.
func mergeNodes(spec model.LabelSpec, list []model.NodeUpsert) []mergedNode {
	index := map[string]int{}
	var out []mergedNode
	for _, n := range list {
		i, ok := index[n.Key]
		if !ok {
			index[n.Key] = len(out)
			m := mergedNode{first: n.SeenAt, last: n.SeenAt}
			m.Label, m.Key = n.Label, n.Key
			m.Props, m.Scalars = map[string]any{}, map[string]model.Scalar{}
			out = append(out, m)
			i = len(out) - 1
		}
		m := &out[i]
		if n.SeenAt.Before(m.first) {
			m.first = n.SeenAt
		}
		if n.SeenAt.After(m.last) {
			m.last = n.SeenAt
		}
		for k, v := range n.Props {
			if v != nil {
				m.Props[k] = v
			}
		}
		for _, f := range spec.Scalars {
			in, ok := n.Scalars[f.Name]
			if !ok || in.Value == nil {
				continue
			}
			cur, has := m.Scalars[f.Name]
			if better(in, cur, has) {
				m.Scalars[f.Name] = in
			}
		}
		for _, f := range n.Facts {
			dup := false
			for _, have := range m.Facts {
				if have == f {
					dup = true
					break
				}
			}
			if !dup {
				m.Facts = append(m.Facts, f)
			}
		}
	}
	return out
}

func nodeRow(spec model.LabelSpec, n mergedNode) map[string]any {
	s := map[string]any{}
	for _, f := range spec.Scalars {
		if sc, ok := n.Scalars[f.Name]; ok && sc.Value != nil {
			s[f.Name] = map[string]any{"v": coerce(sc.Value, f.Type), "c": sc.Confidence, "t": sc.At.UTC()}
		}
	}
	facts := n.Facts
	if facts == nil {
		facts = []string{}
	}
	return map[string]any{"key": n.Key, "first": n.first.UTC(), "last": n.last.UTC(), "props": n.Props, "s": s, "facts": facts}
}

type mergedEdge struct {
	model.EdgeUpsert
	first, last time.Time
	sources     []string
}

// mergeEdges folds upserts of the same relationship into one, for the same
// reason as mergeNodes: the highest confidence wins and, at equal
// confidence, the later upsert.
func mergeEdges(list []model.EdgeUpsert) []mergedEdge {
	index := map[string]int{}
	var out []mergedEdge
	for _, e := range list {
		k := e.FromKey + "\x00" + e.ToKey
		i, ok := index[k]
		if !ok {
			index[k] = len(out)
			out = append(out, mergedEdge{EdgeUpsert: e, first: e.SeenAt, last: e.SeenAt, sources: []string{e.SourceURL}})
			continue
		}
		m := &out[i]
		if e.SeenAt.Before(m.first) {
			m.first = e.SeenAt
		}
		if e.SeenAt.After(m.last) {
			m.last = e.SeenAt
		}
		seen := false
		for _, s := range m.sources {
			if s == e.SourceURL {
				seen = true
				break
			}
		}
		if !seen && len(m.sources) < MaxEdgeSources {
			m.sources = append(m.sources, e.SourceURL)
		}
		if e.Confidence >= m.Confidence {
			m.EdgeUpsert = e
		}
	}
	return out
}

func edgeRow(spec model.EdgeSpec, e mergedEdge) map[string]any {
	props := map[string]any{}
	for _, p := range spec.Props {
		if v, ok := e.Props[p]; ok && v != nil {
			props[p] = v
		}
	}
	return map[string]any{
		"from": e.FromKey, "to": e.ToKey, "first": e.first.UTC(), "last": e.last.UTC(), "confidence": e.Confidence,
		"source_url": e.SourceURL, "sources": e.sources, "props": props,
	}
}

// Plan turns mutations into statements in a fixed, deadlock-averse order:
// node labels in schema order, then relationship shapes in schema order,
// one row per key, sorted by key. Statements are capped at maxRows rows.
// Rows with an unknown label or shape are reported in the returned error
// list and skipped.
func Plan(muts []*model.GraphMutation, maxRows int) ([]Statement, []error) {
	if maxRows <= 0 {
		maxRows = 1000
	}
	var errs []error
	nodes := map[string][]model.NodeUpsert{}
	edges := map[string][]model.EdgeUpsert{}
	for _, m := range muts {
		for _, n := range m.Nodes {
			if _, ok := model.LabelByName(n.Label); !ok || n.Key == "" {
				errs = append(errs, fmt.Errorf("graph: bad node %q/%q from %s", n.Label, n.Key, m.SourceURL))
				continue
			}
			nodes[n.Label] = append(nodes[n.Label], n)
		}
		for _, e := range m.Edges {
			if _, ok := model.EdgeByShape(e.Type, e.FromLabel, e.ToLabel); !ok || e.FromKey == "" || e.ToKey == "" {
				errs = append(errs, fmt.Errorf("graph: bad edge (%s)-[%s]->(%s) from %s", e.FromLabel, e.Type, e.ToLabel, m.SourceURL))
				continue
			}
			k := e.Type + "|" + e.FromLabel + "|" + e.ToLabel
			edges[k] = append(edges[k], e)
		}
	}
	var out []Statement
	for _, spec := range model.Labels {
		list := mergeNodes(spec, nodes[spec.Label])
		if len(list) == 0 {
			continue
		}
		sort.SliceStable(list, func(i, j int) bool { return list[i].Key < list[j].Key })
		cy := NodeCypher(spec)
		for start := 0; start < len(list); start += maxRows {
			end := min(start+maxRows, len(list))
			rows := make([]map[string]any, 0, end-start)
			for _, n := range list[start:end] {
				rows = append(rows, nodeRow(spec, n))
			}
			out = append(out, Statement{Cypher: cy, Rows: rows, Kind: "node", Name: spec.Label})
		}
	}
	for _, spec := range model.Edges {
		list := mergeEdges(edges[spec.Type+"|"+spec.From+"|"+spec.To])
		if len(list) == 0 {
			continue
		}
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].FromKey != list[j].FromKey {
				return list[i].FromKey < list[j].FromKey
			}
			return list[i].ToKey < list[j].ToKey
		})
		cy := EdgeCypher(spec)
		for start := 0; start < len(list); start += maxRows {
			end := min(start+maxRows, len(list))
			rows := make([]map[string]any, 0, end-start)
			for _, e := range list[start:end] {
				rows = append(rows, edgeRow(spec, e))
			}
			out = append(out, Statement{Cypher: cy, Rows: rows, Kind: "edge", Name: spec.Type})
		}
	}
	return out, errs
}

// Transactions groups statements so that no transaction exceeds maxRows
// rows. Order is preserved, so nodes still precede the edges that need them.
func Transactions(stmts []Statement, maxRows int) [][]Statement {
	var out [][]Statement
	var cur []Statement
	rows := 0
	for _, s := range stmts {
		if len(cur) > 0 && rows+len(s.Rows) > maxRows {
			out = append(out, cur)
			cur, rows = nil, 0
		}
		cur = append(cur, s)
		rows += len(s.Rows)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
