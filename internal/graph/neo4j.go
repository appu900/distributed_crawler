package graph

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/pkg/model"
)

// Neo4j is the production graph store.
type Neo4j struct {
	driver neo4j.DriverWithContext
	db     string
	cfg    config.Graph
	log    *slog.Logger
}

// NewNeo4j connects and verifies connectivity.
func NewNeo4j(ctx context.Context, cfg config.Neo4j, g config.Graph, log *slog.Logger) (*Neo4j, error) {
	driver, err := neo4j.NewDriverWithContext(cfg.URI, neo4j.BasicAuth(cfg.Username, cfg.Password, ""))
	if err != nil {
		return nil, fmt.Errorf("neo4j: %w", err)
	}
	vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(vctx); err != nil {
		_ = driver.Close(ctx)
		return nil, fmt.Errorf("neo4j: connecting to %s: %w", cfg.URI, err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Neo4j{driver: driver, db: cfg.Database, cfg: g, log: log}, nil
}

func (n *Neo4j) Close(ctx context.Context) error { return n.driver.Close(ctx) }

// Ping checks connectivity (readiness probe).
func (n *Neo4j) Ping(ctx context.Context) error { return n.driver.VerifyConnectivity(ctx) }

func (n *Neo4j) session(ctx context.Context) neo4j.SessionWithContext {
	return n.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: n.db})
}

func run(ctx context.Context, tx neo4j.ManagedTransaction, cypher string, params map[string]any) ([]*neo4j.Record, error) {
	res, err := tx.Run(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	return res.Collect(ctx)
}

// write runs fn in a write transaction. The driver retries transient errors
// (deadlocks, leader switches) itself; the outer loop adds a bounded number
// of further attempts with backoff for errors that outlast it.
func (n *Neo4j) write(ctx context.Context, fn func(tx neo4j.ManagedTransaction) error) error {
	var err error
	for attempt := 0; ; attempt++ {
		s := n.session(ctx)
		_, err = s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) { return nil, fn(tx) })
		_ = s.Close(ctx)
		if err == nil {
			return nil
		}
		if !neo4j.IsRetryable(err) || attempt >= n.cfg.MaxRetries || ctx.Err() != nil {
			return err
		}
		metrics.GraphRetries.WithLabelValues().Inc()
		wait := time.Duration(100<<min(attempt, 6)) * time.Millisecond
		n.log.Warn("graph transaction failed; retrying", "attempt", attempt+1, "wait", wait, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (n *Neo4j) Apply(ctx context.Context, muts []*model.GraphMutation) error {
	stmts, bad := Plan(muts, n.cfg.BatchRows)
	if len(bad) > 0 {
		return errors.Join(bad...)
	}
	for _, group := range Transactions(stmts, n.cfg.BatchRows) {
		start := time.Now()
		err := n.write(ctx, func(tx neo4j.ManagedTransaction) error {
			for _, st := range group {
				res, err := tx.Run(ctx, st.Cypher, map[string]any{
					"rows": st.Rows, "maxFacts": n.cfg.MaxFacts, "maxSources": MaxEdgeSources,
				})
				if err != nil {
					return fmt.Errorf("%s %s: %w", st.Kind, st.Name, err)
				}
				if _, err := res.Consume(ctx); err != nil {
					return fmt.Errorf("%s %s: %w", st.Kind, st.Name, err)
				}
			}
			return nil
		})
		metrics.GraphWriteDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.GraphErrors.WithLabelValues().Inc()
			return fmt.Errorf("neo4j: apply: %w", err)
		}
		for _, st := range group {
			metrics.GraphRows.WithLabelValues(st.Kind).Add(float64(len(st.Rows)))
		}
	}
	return nil
}

const pageProjection = `pg{.url, .raw_key, .raw_offset, .raw_length, .extract_key}`

func pageRefs(v any) []PageRef {
	list, _ := v.([]any)
	var out []PageRef
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ref := PageRef{}
		ref.URL, _ = m["url"].(string)
		ref.RawKey, _ = m["raw_key"].(string)
		ref.RawOffset, _ = m["raw_offset"].(int64)
		ref.RawLength, _ = m["raw_length"].(int64)
		ref.ExtractKey, _ = m["extract_key"].(string)
		if ref.URL != "" {
			out = append(out, ref)
		}
	}
	return out
}

const redactPagesCypher = `UNWIND $urls AS u
MATCH (pg:Page {url: u})
REMOVE pg.raw_key, pg.raw_key_conf, pg.raw_key_at, pg.raw_offset, pg.raw_offset_conf, pg.raw_offset_at,
       pg.raw_length, pg.raw_length_conf, pg.raw_length_at, pg.extract_key, pg.extract_key_conf, pg.extract_key_at,
       pg.content_hash, pg.content_hash_conf, pg.content_hash_at
SET pg.redacted = true`

func redact(ctx context.Context, tx neo4j.ManagedTransaction, pages []PageRef) error {
	if len(pages) == 0 {
		return nil
	}
	urls := make([]string, len(pages))
	for i, p := range pages {
		urls[i] = p.URL
	}
	_, err := run(ctx, tx, redactPagesCypher, map[string]any{"urls": urls})
	return err
}

func erasePersonTx(ctx context.Context, tx neo4j.ManagedTransaction, id string) (EraseReport, error) {
	var rep EraseReport
	recs, err := run(ctx, tx, `MATCH (p:Person {id: $id})
OPTIONAL MATCH (pg:Page)-[:MENTIONS]->(p)
RETURN p.full_name AS name, p.company_id AS company, collect(DISTINCT `+pageProjection+`) AS pages`, map[string]any{"id": id})
	if err != nil || len(recs) == 0 {
		return rep, err
	}
	name, _, _ := neo4j.GetRecordValue[string](recs[0], "name")
	company, _, _ := neo4j.GetRecordValue[string](recs[0], "company")
	pages, _ := recs[0].Get("pages")
	rep.People = append(rep.People, ErasedPerson{ID: id, CompanyID: company, Name: name})
	rep.Pages = pageRefs(pages)

	// Contact nodes nobody else owns go with the person.
	owned, err := run(ctx, tx, `MATCH (p:Person {id: $id})-[:HAS_EMAIL|HAS_PHONE|HAS_SOCIAL]->(x)
WHERE NOT EXISTS { MATCH (o)-[r]->(x) WHERE o <> p AND type(r) <> 'MENTIONS' }
OPTIONAL MATCH (pg:Page)-[:MENTIONS]->(x)
RETURN elementId(x) AS eid, x.address AS address, collect(DISTINCT `+pageProjection+`) AS pages`, map[string]any{"id": id})
	if err != nil {
		return rep, err
	}
	var eids []string
	for _, r := range owned {
		eid, _, _ := neo4j.GetRecordValue[string](r, "eid")
		eids = append(eids, eid)
		if addr, isNil, _ := neo4j.GetRecordValue[string](r, "address"); !isNil && addr != "" {
			rep.Emails = append(rep.Emails, addr)
		}
		pg, _ := r.Get("pages")
		rep.Pages = append(rep.Pages, pageRefs(pg)...)
	}
	if err := redact(ctx, tx, rep.Pages); err != nil {
		return rep, err
	}
	if _, err := run(ctx, tx, `MATCH (p:Person {id: $id}) DETACH DELETE p`, map[string]any{"id": id}); err != nil {
		return rep, err
	}
	rep.Nodes = 1 + len(eids)
	if len(eids) > 0 {
		if _, err := run(ctx, tx, `MATCH (x) WHERE elementId(x) IN $eids DETACH DELETE x`, map[string]any{"eids": eids}); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

func (n *Neo4j) ErasePerson(ctx context.Context, personID string) (EraseReport, error) {
	var rep EraseReport
	err := n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		var err error
		rep, err = erasePersonTx(ctx, tx, personID)
		return err
	})
	return rep, err
}

func (n *Neo4j) EraseEmail(ctx context.Context, address string, cascade bool) (EraseReport, error) {
	var rep EraseReport
	err := n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		rep = EraseReport{}
		recs, err := run(ctx, tx, `MATCH (e:Email {address: $a})
OPTIONAL MATCH (p:Person)-[:HAS_EMAIL]->(e)
OPTIONAL MATCH (pg:Page)-[:MENTIONS]->(e)
RETURN e.kind AS kind, collect(DISTINCT p.id) AS owners, collect(DISTINCT `+pageProjection+`) AS pages`, map[string]any{"a": address})
		if err != nil || len(recs) == 0 {
			return err
		}
		kind, _, _ := neo4j.GetRecordValue[string](recs[0], "kind")
		owners, _ := recs[0].Get("owners")
		pages, _ := recs[0].Get("pages")
		rep.Pages = pageRefs(pages)
		rep.Emails = []string{address}
		if err := redact(ctx, tx, rep.Pages); err != nil {
			return err
		}
		if _, err := run(ctx, tx, `MATCH (e:Email {address: $a}) DETACH DELETE e`, map[string]any{"a": address}); err != nil {
			return err
		}
		rep.Nodes = 1
		if cascade && kind == model.EmailPersonal {
			list, _ := owners.([]any)
			for _, o := range list {
				if id, ok := o.(string); ok {
					sub, err := erasePersonTx(ctx, tx, id)
					if err != nil {
						return err
					}
					rep.merge(sub)
				}
			}
		}
		return nil
	})
	return rep, err
}

func (n *Neo4j) PurgeCompany(ctx context.Context, companyID string) (EraseReport, error) {
	var rep EraseReport
	params := map[string]any{"id": companyID}
	err := n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		rep = EraseReport{}
		recs, err := run(ctx, tx, `MATCH (c:Company {id: $id})
OPTIONAL MATCH (c)-[:EMPLOYS]->(p:Person)
RETURN collect(DISTINCT p.id) AS people`, params)
		if err != nil || len(recs) == 0 {
			return err
		}
		people, _ := recs[0].Get("people")
		list, _ := people.([]any)
		for _, p := range list {
			if id, ok := p.(string); ok {
				sub, err := erasePersonTx(ctx, tx, id)
				if err != nil {
					return err
				}
				rep.merge(sub)
			}
		}
		recs, err = run(ctx, tx, `MATCH (c:Company {id: $id})
OPTIONAL MATCH (c)-[:HAS_EMAIL|HAS_PHONE|HAS_SOCIAL|LOCATED_AT]->(x)
OPTIONAL MATCH (c)-[:HAS_DOMAIN]->(:Domain)-[:HAS_PAGE]->(pg:Page)
RETURN collect(DISTINCT elementId(x)) AS eids, collect(DISTINCT `+pageProjection+`) AS pages`, params)
		if err != nil || len(recs) == 0 {
			return err
		}
		eids, _ := recs[0].Get("eids")
		pages, _ := recs[0].Get("pages")
		rep.Pages = append(rep.Pages, pageRefs(pages)...)
		res, err := tx.Run(ctx, `MATCH (c:Company {id: $id})
OPTIONAL MATCH (c)-[:HAS_DOMAIN]->(d:Domain)
OPTIONAL MATCH (d)-[:HAS_PAGE]->(pg:Page)
DETACH DELETE pg, d, c`, params)
		if err != nil {
			return err
		}
		sum, err := res.Consume(ctx)
		if err != nil {
			return err
		}
		rep.Nodes += sum.Counters().NodesDeleted()
		recs, err = run(ctx, tx, `MATCH (x) WHERE elementId(x) IN $eids AND NOT (x)--()
WITH x, x.address AS address
DELETE x
RETURN address`, map[string]any{"eids": eids})
		if err != nil {
			return err
		}
		for _, r := range recs {
			rep.Nodes++
			if addr, isNil, _ := neo4j.GetRecordValue[string](r, "address"); !isNil && addr != "" {
				rep.Emails = append(rep.Emails, addr)
			}
		}
		return nil
	})
	return rep, err
}

func (n *Neo4j) FlagSuppressed(ctx context.Context, kind, member string, at time.Time) error {
	return n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		_, err := run(ctx, tx, `MERGE (s:Suppression {kind: $kind, member: $member}) ON CREATE SET s.created_at = $at`,
			map[string]any{"kind": kind, "member": member, "at": at.UTC()})
		return err
	})
}

func (n *Neo4j) UnflagSuppressed(ctx context.Context, kind, member string) error {
	return n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		_, err := run(ctx, tx, `MATCH (s:Suppression {kind: $kind, member: $member}) DELETE s`,
			map[string]any{"kind": kind, "member": member})
		return err
	})
}

// Exec runs one write statement in its own transaction.
func (n *Neo4j) Exec(ctx context.Context, cypher string, params map[string]any) error {
	return n.write(ctx, func(tx neo4j.ManagedTransaction) error {
		_, err := run(ctx, tx, cypher, params)
		return err
	})
}

// Query runs a read query and returns its records as maps.
func (n *Neo4j) Query(ctx context.Context, cypher string, params map[string]any) ([]map[string]any, error) {
	s := n.session(ctx)
	defer s.Close(ctx)
	out, err := s.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		recs, err := run(ctx, tx, cypher, params)
		if err != nil {
			return nil, err
		}
		rows := make([]map[string]any, len(recs))
		for i, r := range recs {
			rows[i] = r.AsMap()
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]map[string]any), nil
}

func (n *Neo4j) Suppressed(ctx context.Context) (map[string][]string, error) {
	rows, err := n.Query(ctx, `MATCH (s:Suppression) RETURN s.kind AS kind, s.member AS member ORDER BY kind, member`, nil)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, r := range rows {
		kind, _ := r["kind"].(string)
		member, _ := r["member"].(string)
		out[kind] = append(out[kind], member)
	}
	return out, nil
}

func (n *Neo4j) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Nodes: map[string]int64{}, Edges: map[string]int64{}}
	rows, err := n.Query(ctx, `MATCH (n) RETURN labels(n)[0] AS k, count(*) AS c`, nil)
	if err != nil {
		return st, err
	}
	for _, r := range rows {
		k, _ := r["k"].(string)
		st.Nodes[k], _ = r["c"].(int64)
	}
	rows, err = n.Query(ctx, `MATCH ()-[r]->() RETURN type(r) AS k, count(*) AS c`, nil)
	if err != nil {
		return st, err
	}
	for _, r := range rows {
		k, _ := r["k"].(string)
		st.Edges[k], _ = r["c"].(int64)
	}
	return st, nil
}
