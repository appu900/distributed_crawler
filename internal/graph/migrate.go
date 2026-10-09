package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Migration is one versioned schema change. Every statement must be
// idempotent (IF NOT EXISTS), so a migration interrupted half-way, or run by
// two processes at once, can simply be run again.
type Migration struct {
	Version    int
	Name       string
	Statements []string
}

// Migrations is the ordered schema history. Never edit a released entry;
// append a new one.
var Migrations = []Migration{
	{Version: 1, Name: "uniqueness constraints", Statements: []string{
		`CREATE CONSTRAINT schema_migration_version IF NOT EXISTS FOR (n:SchemaMigration) REQUIRE n.version IS UNIQUE`,
		`CREATE CONSTRAINT company_id IF NOT EXISTS FOR (n:Company) REQUIRE n.id IS UNIQUE`,
		`CREATE CONSTRAINT domain_name IF NOT EXISTS FOR (n:Domain) REQUIRE n.name IS UNIQUE`,
		`CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE`,
		`CREATE CONSTRAINT email_address IF NOT EXISTS FOR (n:Email) REQUIRE n.address IS UNIQUE`,
		`CREATE CONSTRAINT phone_e164 IF NOT EXISTS FOR (n:Phone) REQUIRE n.e164 IS UNIQUE`,
		`CREATE CONSTRAINT social_url IF NOT EXISTS FOR (n:SocialProfile) REQUIRE n.url IS UNIQUE`,
		`CREATE CONSTRAINT technology_name IF NOT EXISTS FOR (n:Technology) REQUIRE n.name IS UNIQUE`,
		`CREATE CONSTRAINT location_id IF NOT EXISTS FOR (n:Location) REQUIRE n.id IS UNIQUE`,
		`CREATE CONSTRAINT page_url IF NOT EXISTS FOR (n:Page) REQUIRE n.url IS UNIQUE`,
	}},
	{Version: 2, Name: "lookup and fulltext indexes", Statements: []string{
		`CREATE INDEX company_name IF NOT EXISTS FOR (n:Company) ON (n.name)`,
		`CREATE INDEX person_full_name IF NOT EXISTS FOR (n:Person) ON (n.full_name)`,
		`CREATE INDEX location_country IF NOT EXISTS FOR (n:Location) ON (n.country)`,
		`CREATE FULLTEXT INDEX company_search IF NOT EXISTS FOR (n:Company) ON EACH [n.name, n.description]`,
	}},
	{Version: 3, Name: "suppression flags and secondary lookups", Statements: []string{
		`CREATE CONSTRAINT suppression_key IF NOT EXISTS FOR (n:Suppression) REQUIRE (n.kind, n.member) IS UNIQUE`,
		`CREATE INDEX email_domain IF NOT EXISTS FOR (n:Email) ON (n.domain)`,
		`CREATE INDEX person_seniority IF NOT EXISTS FOR (n:Person) ON (n.seniority)`,
		`CREATE INDEX company_country IF NOT EXISTS FOR (n:Company) ON (n.country)`,
	}},
}

// Migrate applies every migration that has not been recorded yet and returns
// the versions it applied.
func (n *Neo4j) Migrate(ctx context.Context) ([]int, error) {
	applied := map[int64]bool{}
	rows, err := n.Query(ctx, `MATCH (m:SchemaMigration) RETURN m.version AS v`, nil)
	if err != nil {
		return nil, fmt.Errorf("migrate: reading applied versions: %w", err)
	}
	for _, r := range rows {
		if v, ok := r["v"].(int64); ok {
			applied[v] = true
		}
	}
	var done []int
	for _, m := range Migrations {
		if applied[int64(m.Version)] {
			continue
		}
		for _, stmt := range m.Statements {
			// Schema changes cannot share a transaction with data writes
			// and each runs in its own implicit transaction.
			s := n.session(ctx)
			_, err := s.Run(ctx, stmt, nil)
			_ = s.Close(ctx)
			if err != nil {
				return done, fmt.Errorf("migrate: v%d (%s): %w", m.Version, m.Name, err)
			}
		}
		err := n.write(ctx, func(tx neo4j.ManagedTransaction) error {
			_, err := run(ctx, tx, `MERGE (m:SchemaMigration {version: $v}) ON CREATE SET m.name = $name, m.applied_at = $at`,
				map[string]any{"v": m.Version, "name": m.Name, "at": time.Now().UTC()})
			return err
		})
		if err != nil {
			return done, fmt.Errorf("migrate: recording v%d: %w", m.Version, err)
		}
		n.log.Info("migration applied", "version", m.Version, "name", m.Name)
		done = append(done, m.Version)
	}
	// Index population is asynchronous; wait so the first writes use them.
	s := n.session(ctx)
	defer s.Close(ctx)
	if _, err := s.Run(ctx, `CALL db.awaitIndexes(120)`, nil); err != nil {
		return done, fmt.Errorf("migrate: waiting for indexes: %w", err)
	}
	return done, nil
}
