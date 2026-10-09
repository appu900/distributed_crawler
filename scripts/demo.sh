#!/bin/sh
# Demo: crawl the bundled fixture sites (acme.test, globex.test) served from
# a local port, then print graph statistics and a few example queries.
#
# Needs Docker (for redis, neo4j, minio) and Go. Run from the repo root:
#   ./scripts/demo.sh
set -eu

cd "$(dirname "$0")/.."
SITE_ADDR="${SITE_ADDR:-127.0.0.1:8089}"

[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }
set -a; . ./.env; set +a

echo "==> starting redis, neo4j and minio"
docker compose up -d --wait redis neo4j minio

echo "==> building"
mkdir -p bin
go build -o bin/leadgraph ./cmd/leadgraph
go build -o bin/fixturesite ./cmd/fixturesite

echo "==> starting fixture sites on $SITE_ADDR"
./bin/fixturesite -addr "$SITE_ADDR" &
SITE_PID=$!
trap 'kill $SITE_PID 2>/dev/null || true' EXIT INT TERM
sleep 1

# The demo uses its own Redis key prefix and S3 bucket, and a fresh crawl
# epoch per run, so it never mixes with a real crawl and can be repeated.
export LEADGRAPH_REDIS_KEY_PREFIX=lgdemo
export LEADGRAPH_S3_BUCKET=leadgraph-demo
export LEADGRAPH_CRAWL_EPOCH="demo-$(date +%s)"
export LEADGRAPH_NEO4J_PASSWORD="$NEO4J_PASSWORD"
export LEADGRAPH_S3_ACCESS_KEY="$MINIO_ROOT_USER"
export LEADGRAPH_S3_SECRET_KEY="$MINIO_ROOT_PASSWORD"
# Fixture hosts resolve to the local server; that needs private addresses,
# which the crawler refuses by default.
export LEADGRAPH_FETCH_ALLOW_PRIVATE_NETWORKS=true
export LEADGRAPH_FETCH_HOST_OVERRIDES="acme.test=$SITE_ADDR,www.acme.test=$SITE_ADDR,globex.test=$SITE_ADDR"
export LEADGRAPH_EXTRACT_ALLOW_RESERVED_TLDS=true
export LEADGRAPH_CRAWL_DEFAULT_DELAY=200ms
export LEADGRAPH_STORE_FLUSH_INTERVAL=200ms
export LEADGRAPH_TELEMETRY_LOG_LEVEL=warn
export LEADGRAPH_TELEMETRY_LOG_FORMAT=text

echo "==> crawling http://acme.test and http://globex.test"
./bin/leadgraph run-all --exit-when-idle http://acme.test http://globex.test

echo
echo "==> statistics"
./bin/leadgraph stats

cypher() {
  docker compose exec -T neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain "$1"
}
echo
echo "==> people and their contact details"
cypher "MATCH (c:Company)-[e:EMPLOYS]->(p:Person)
        OPTIONAL MATCH (p)-[:HAS_EMAIL]->(m:Email)
        RETURN c.name AS company, p.full_name AS person, e.title AS title, p.seniority AS seniority, collect(m.address) AS emails
        ORDER BY company, person"
echo
echo "==> decision makers at companies using HubSpot in the US"
cypher "MATCH (:Technology {name: 'HubSpot'})<-[:USES]-(c:Company)-[:LOCATED_AT]->(:Location {country: 'US'})
        MATCH (c)-[:EMPLOYS]->(p:Person) WHERE p.seniority IN ['founder', 'c_level', 'vp', 'director']
        RETURN DISTINCT c.name AS company, p.full_name AS person, p.title AS title ORDER BY person"
echo
echo "==> who links to whom"
cypher "MATCH (a:Company)-[l:LINKS_TO]->(b:Company) RETURN a.name AS source, b.name AS target, l.source_url AS seen_on"

echo
echo "The demo graph is now in Neo4j (http://localhost:7474). It shares the"
echo "database with real crawls: run 'make demo-clean' before crawling real sites."
