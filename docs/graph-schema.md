# Graph schema

LeadGraph stores what it finds as a property graph in Neo4j 5. The schema is
defined in one place in code, [`pkg/model/graph.go`](../pkg/model/graph.go);
the Cypher the writer runs is generated from it
([`internal/graph/cypher.go`](../internal/graph/cypher.go)).

## Nodes

| Label | Key | Properties |
|---|---|---|
| `Company` | `id` = hash(registrable domain) | `domain`, `name`, `description`, `industry`, `size_hint`, `founded`, `country`, `facts`, `first_seen`, `last_seen` |
| `Domain` | `name` (hostname) | `registrable`, `tld`, `first_seen`, `last_seen` |
| `Person` | `id` = hash(company id + name + (LinkedIn URL or title)) | `company_id`, `full_name`, `title`, `seniority`, `facts`, `first_seen`, `last_seen` |
| `Email` | `address` (lower case) | `id` (hash, as used by the suppression list), `kind` (`role`\|`personal`), `status` (`unverified`), `domain`, `disposable` |
| `Phone` | `e164` | `kind` (`mobile`, `landline`, `landline_or_mobile`, `toll_free`, `voip`, `fax`, `other`) |
| `SocialProfile` | `url` (canonical) | `network` (`linkedin`, `x`, `facebook`, `instagram`, `github`, `youtube`), `handle` |
| `Technology` | `name` | `category` |
| `Location` | `id` = hash(country, region, city, postal, street) | `country` (ISO alpha-2), `region`, `city`, `postal`, `street` |
| `Page` | `url` (normalized, after redirects) | `type` (`home`\|`about`\|`team`\|`contact`\|`careers`\|`other`), `status`, `skip_reason`, `content_hash`, `fetched_at`, `title`, `raw_key`, `raw_offset`, `raw_length`, `extract_key`, `redacted` |

Two bookkeeping labels exist beside the data: `SchemaMigration {version, name,
applied_at}` and `Suppression {kind, member, created_at}`.

`Person.seniority` is one of `founder`, `c_level`, `vp`, `director`, `manager`,
`individual`, `entry`.

### Merge-ruled properties

`Company.{name, description, industry, size_hint, founded, country}`,
`Person.{full_name, title, seniority}` and most `Page` properties are
*merge-ruled*. Each has two companions, `<prop>_conf` (0..1) and `<prop>_at`
(datetime), and is only replaced when the incoming fact has

- strictly higher confidence, or
- equal confidence and a timestamp that is not older.

A lower-confidence fact never replaces a higher one. The timestamp is the
fetch time of the page the fact came from, so re-processing the same fetch
rewrites the same values.

### Provenance: `facts`

`Company` and `Person` keep every value ever seen, with its provenance, in
`facts`: a list of JSON strings, newest last, capped at `graph.max_facts`
(default 50).

```json
{"field":"name","value":"Acme Robotics","source_url":"https://acme.com/","extractor":"jsonld.organization","confidence":0.95,"extracted_at":"2026-03-01T10:00:00Z"}
```

Neo4j cannot store maps inside a list, hence the JSON encoding. Decode in the
client, or filter with string predicates:

```cypher
MATCH (c:Company {name: 'Acme Robotics'})
UNWIND c.facts AS f
WITH f WHERE f CONTAINS '"field":"name"'
RETURN f
```

## Relationships

Every relationship carries `source_url` (the page behind the highest-confidence
sighting), `sources` (up to 20 pages it was seen on), `confidence`,
`first_seen` and `last_seen`.

| Relationship | Extra properties |
|---|---|
| `(Company)-[:HAS_DOMAIN]->(Domain)` | |
| `(Domain)-[:HAS_PAGE]->(Page)` | |
| `(Company)-[:EMPLOYS]->(Person)` | `title` |
| `(Company)-[:HAS_EMAIL]->(Email)` | |
| `(Person)-[:HAS_EMAIL]->(Email)` | |
| `(Company)-[:HAS_PHONE]->(Phone)` | |
| `(Person)-[:HAS_PHONE]->(Phone)` | |
| `(Company)-[:HAS_SOCIAL]->(SocialProfile)` | |
| `(Person)-[:HAS_SOCIAL]->(SocialProfile)` | |
| `(Company)-[:USES]->(Technology)` | |
| `(Company)-[:LOCATED_AT]->(Location)` | |
| `(Page)-[:MENTIONS]->(Person)` | |
| `(Page)-[:MENTIONS]->(Email)` | |
| `(Page)-[:MENTIONS]->(Phone)` | |
| `(Company)-[:LINKS_TO]->(Company)` | |

Notes:

- An email or phone found inside a person's card, or a personal address whose
  local part matches a known person's name (`first.last`, `flast`, …), hangs
  off the `Person`. Everything else hangs off the `Company`.
- `LINKS_TO` only exists between companies that have both been crawled. A link
  to a company crawled later is remembered and added when that company's
  homepage is resolved, so crawl order does not matter.
- `MENTIONS` is the provenance index used for erasure: it answers "which
  stored pages contain this person?".

## Constraints and indexes

Created by `leadgraph migrate` (and automatically by any stage that writes).
Migrations are versioned in
[`internal/graph/migrate.go`](../internal/graph/migrate.go) and idempotent.

| Version | Contents |
|---|---|
| 1 | Uniqueness: `Company.id`, `Domain.name`, `Person.id`, `Email.address`, `Phone.e164`, `SocialProfile.url`, `Technology.name`, `Location.id`, `Page.url` |
| 2 | Indexes: `Company.name`, `Person.full_name`, `Location.country`; fulltext `company_search` on `Company.name, Company.description` |
| 3 | Uniqueness on `Suppression(kind, member)`; indexes on `Email.domain`, `Person.seniority`, `Company.country` |

## Example queries

### Decision makers at companies using technology X in country Y

```cypher
MATCH (:Technology {name: $tech})<-[:USES]-(c:Company)-[:LOCATED_AT]->(:Location {country: $country})
MATCH (c)-[e:EMPLOYS]->(p:Person)
WHERE p.seniority IN ['founder', 'c_level', 'vp', 'director']
OPTIONAL MATCH (p)-[:HAS_EMAIL]->(m:Email)
RETURN c.name AS company, p.full_name AS person, e.title AS title,
       collect(DISTINCT m.address) AS emails, e.source_url AS seen_on, e.confidence AS confidence
ORDER BY company, confidence DESC
```

`$tech = 'HubSpot'`, `$country = 'US'`. `Company.country` holds the same ISO
code when you do not need the address:

```cypher
MATCH (:Technology {name: $tech})<-[:USES]-(c:Company {country: $country}) RETURN c.name
```

### Companies linked from competitor Z

Outbound links are a cheap signal for partners, customers and competitors.

```cypher
MATCH (z:Company)-[:HAS_DOMAIN]->(:Domain {registrable: $domain})
MATCH (z)-[l:LINKS_TO]->(c:Company)
RETURN c.name AS company, c.domain AS domain, l.source_url AS linked_from
ORDER BY company
```

And the reverse — who links to Z:

```cypher
MATCH (c:Company)-[:LINKS_TO]->(:Company {domain: $domain})
RETURN c.name, c.domain
```

### Everything known about one company

```cypher
MATCH (c:Company {domain: $domain})
OPTIONAL MATCH (c)-[:EMPLOYS]->(p:Person)
OPTIONAL MATCH (c)-[:HAS_EMAIL]->(e:Email)
OPTIONAL MATCH (c)-[:HAS_PHONE]->(ph:Phone)
OPTIONAL MATCH (c)-[:USES]->(t:Technology)
OPTIONAL MATCH (c)-[:LOCATED_AT]->(l:Location)
RETURN c.name, c.description, c.industry, c.size_hint, c.founded,
       collect(DISTINCT p.full_name + ' (' + coalesce(p.title, '?') + ')') AS people,
       collect(DISTINCT e.address) AS emails, collect(DISTINCT ph.e164) AS phones,
       collect(DISTINCT t.name) AS technologies, collect(DISTINCT l.city + ', ' + l.country) AS locations
```

### Full-text company search

```cypher
CALL db.index.fulltext.queryNodes('company_search', $q) YIELD node, score
RETURN node.name, node.description, score
ORDER BY score DESC LIMIT 20
```

### Role mailboxes only

```cypher
MATCH (c:Company)-[r:HAS_EMAIL]->(e:Email {kind: 'role'})
WHERE NOT e.disposable AND r.confidence >= 0.8
RETURN c.name, e.address, r.source_url
```

### Where did this fact come from?

```cypher
MATCH (p:Person {id: $person_id})<-[m:MENTIONS]-(pg:Page)
RETURN pg.url, pg.fetched_at, pg.raw_key, pg.raw_offset, pg.raw_length, m.confidence
ORDER BY pg.fetched_at DESC
```

`raw_key`/`raw_offset`/`raw_length` address the exact stored response, so a
fact can be traced back to the bytes it was extracted from.

### Low-confidence people worth reviewing

```cypher
MATCH (c:Company)-[e:EMPLOYS]->(p:Person)
WHERE e.confidence < 0.6
RETURN c.name, p.full_name, p.title, e.confidence, e.source_url
ORDER BY e.confidence
```

### Technology adoption by country

```cypher
MATCH (t:Technology)<-[:USES]-(c:Company)
WHERE c.country IS NOT NULL
RETURN t.category, t.name, c.country, count(*) AS companies
ORDER BY companies DESC LIMIT 50
```

### Stale companies (not seen for 90 days)

```cypher
MATCH (c:Company)
WHERE c.last_seen < datetime() - duration('P90D')
RETURN c.name, c.domain, c.last_seen
ORDER BY c.last_seen
```

### Pages that could not be fetched, and why

```cypher
MATCH (:Domain {registrable: $domain})-[:HAS_PAGE]->(pg:Page)
WHERE pg.skip_reason <> '' OR pg.status >= 400
RETURN pg.url, pg.status, pg.skip_reason
```

`skip_reason` is one of `robots_disallowed`, `content_type`,
`redirect_off_domain`, `too_many_redirects`, `fetch_failed`, `circuit_open`,
`robots_unavailable`, `blocked_private_address`.

## Erasure

`leadgraph erase --email X` and `leadgraph erase --person-id X`:

1. add the hashed email / person ID (and, for a person, the hash of
   company + name) to the suppression list, in Redis and as `Suppression`
   nodes — *before* deleting, so a running pipeline cannot write them back;
2. `DETACH DELETE` the node, plus the email, phone and social nodes that only
   that person owned;
3. for every `Page` that `MENTIONS` the erased node: overwrite the raw record
   with a same-length tombstone, delete the extraction objects, clear the
   page's `raw_*`, `extract_key` and `content_hash`, and set `redacted = true`.

With `--email`, the person who owns a *personal* address is erased too unless
`--no-cascade` is given.
