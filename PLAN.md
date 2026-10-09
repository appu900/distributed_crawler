# LeadGraph — implementation plan

A staged crawl pipeline that turns seed domains into a provenance-carrying
property graph of B2B company data.

```
seeds → [Frontier] → [Fetcher] → [Parser/Extractor] → [Resolver] → [Graph Writer]
                         ↓              ↓
                    raw → S3      extraction JSON → S3
```

## Packages

| Package | Responsibility |
|---|---|
| `cmd/leadgraph` | Single binary: `seed, frontier, fetch, parse, resolve, write, run-all, dlq, migrate, suppress, erase, kill, stats, rebuild, config` |
| `cmd/fixturesite` | Local multi-host fixture web server used by the demo |
| `pkg/model` | Wire types: `Fact`, `Extraction`, `FetchRequest`, `FetchResult`, `GraphMutation`, graph schema table |
| `internal/config` | YAML + env config, defaults, validation |
| `internal/urlx` | URL normalization, registrable domain, hashing |
| `internal/normalize` | Company/person name normalization, Jaro-Winkler, country and seniority mapping |
| `internal/extract` | Pure extractors (JSON-LD, meta, email, phone, social, address, people, company, tech, links, page type) |
| `internal/robots` | robots.txt fetch, Redis-backed cache, allow/crawl-delay/sitemaps |
| `internal/fetch` | HTTP client, retries, circuit breaker, proxy pool, headless fallback, fetch stage worker |
| `internal/frontier` | Dedup, per-host politeness ZSETs, priority, budgets, dispatcher, seeding, kill switch |
| `internal/queue` | Redis Streams producer/consumer: groups, ack, reclaim, retry counting, DLQ, backpressure |
| `internal/store` | Blob interface (S3/MinIO + in-memory), WARC-like gzip-member JSONL batches, tombstoning |
| `internal/parse` | Parse stage worker (raw → extraction → S3 → resolve stream, links → frontier), sitemap parsing |
| `internal/resolve` | Deterministic IDs, merge rules, person dedupe, geo policy, `GraphMutation` building, stage worker |
| `internal/graph` | Cypher generation, Neo4j sink, in-memory sink, migrations, writer stage, erase, stats |
| `internal/suppress` | Suppression list (Redis sets, mirrored to the graph) |
| `internal/erase` | Erasure across graph, raw store, extractions and resolver state |
| `internal/dlq` | DLQ list / replay / purge |
| `internal/metrics`, `internal/obs` | Prometheus metrics + HTTP server; slog and OpenTelemetry setup |
| `internal/pipeline` | Wires stages together; idle detection; used by the CLI and by tests |
| `internal/testsite` | Embedded fixture sites (multi-page, robots, redirects, slow, 429, malformed) |

## Build order

1. models, URL normalization, name normalization
2. extractors + fixtures + fuzz + benchmark
3. robots, HTTP client, breaker, fetch worker
4. blob store + raw batches
5. queue plumbing, frontier
6. parse worker, resolver
7. graph schema, Cypher, Neo4j sink, migrations, writer
8. suppression, erase, DLQ
9. pipeline wiring, CLI, observability
10. integration + chaos tests, demo, docs, CI

After each stage: `go build ./... && go vet ./... && go test -race ./...`.

## Decisions and assumptions

These are choices the brief left open, or places where I deviated on purpose.

- **Module name.** The repo had an untracked `go.mod` declaring
  `github.com/appu900/crawler`. The brief fixes the name as
  `github.com/appu/leadgraph`, so it was replaced.
- **Go version.** The `go` directive is whatever the dependency set requires
  (testcontainers and OpenTelemetry need newer than 1.23). Built with 1.26.
- **Streams are bounded by producer blocking, not `MAXLEN` trimming.** Trimming
  a stream drops unprocessed work, which contradicts "loses no work". Producers
  wait while the downstream stream is at its max length. Consumers `XACK`+`XDEL`
  in one transaction, so `XLEN` is the true backlog (undelivered + pending) and
  is what the lag metric and backpressure read. One consumer group per stream.
- **Retry model.** A failed handler leaves the message pending. Any consumer
  reclaims it with `XAUTOCLAIM` after `claim_min_idle`; the delivery counter
  decides when it moves to the DLQ stream.
- **Frontier is ZSETs, not a stream.** `lg:hosts` holds each host scored by its
  next-allowed fetch time; `lg:fq:{host}` holds that host's URLs scored by
  priority. A Lua script atomically pops due hosts and `XADD`s to the fetch
  stream. The host is leased until the fetcher releases it with
  `finished_at + delay`, so there is never more than one in-flight request per
  host. The 40-page budget is enforced at dispatch, so priority decides which
  40 pages are fetched rather than discovery order.
- **Global rate limit** is a per-second Redis counter checked inside the
  dispatch script, so it holds across any number of processes.
- **Dedup** uses RedisBloom (`BF.ADD`) when the module is present and falls back
  to a plain set. Keys include a configurable crawl *epoch*; bump it to recrawl.
- **Raw store format.** Each record is its own gzip member; a batch object is
  the concatenation (like WARC), so `zcat` yields JSONL and one record can be
  read with a ranged GET. Appends are group-committed: the fetcher only
  publishes a `FetchResult` after the batch holding its record is durable.
- **Erasure of raw data.** A raw record is replaced in place by a tombstone of
  exactly the same byte length (a tiny gzip member plus empty padding members
  carrying gzip "extra" header bytes), so other records' offsets stay valid
  and `zcat` still reads the object. The whole page record is redacted, not
  just the one person's fragment.
- **Redirects go through the frontier.** The fetch stage does not follow a
  redirect in place; it enqueues the target (same depth, hop count + 1). The
  target is then deduplicated against URLs already crawled, checked against
  its own robots.txt and fetched under its host's delay. Following in the
  HTTP client fetched `/team` twice when `/old-team` redirected to it. Only
  robots.txt requests follow redirects directly (same site, max 5).
- **MinIO image.** The official `minio/minio` image is no longer on Docker
  Hub. Compose and the integration tests default to Chainguard's build
  (`cgr.dev/chainguard/minio`) and take `MINIO_IMAGE` /
  `LEADGRAPH_TEST_MINIO_IMAGE` overrides.
- **Headless fallback** triggers only for an "empty shell": under
  `min_text_bytes` of visible text *and* at least one external or module
  script, and its result is kept only if it has more text than the static
  response. The browser gets the same host overrides and User-Agent as the
  HTTP client.
- **Merge rules live in Cypher** (and identically in the in-memory sink): a
  scalar is replaced only by a strictly higher confidence, or equal confidence
  and newer timestamp. `extracted_at` is the page's fetch time, which makes
  re-processing the same fetch produce byte-identical facts (idempotence).
- **Person identity.** ID is `hash(company_id + name + (linkedin | title))` as
  specified. Since a title differs between pages, a per-company registry in
  Redis maps fuzzy-equal names (Jaro-Winkler) to the first ID seen. Two people
  are kept apart only when both carry different LinkedIn URLs.
- **Emails are never guessed.** The resolver may *attach* an email that was
  actually found on the site to a person when the local part matches their name
  (`first.last`, `flast`, …); it never synthesizes addresses.
- **Suppression list stores hashes** for emails and people, so the list itself
  holds no personal data. Erasing a person also suppresses
  `company + normalized name`, because the person ID changes with their title.
- **Country policy** is decided per company from postal address, then ccTLD,
  then phone region. A denied company is purged and its domain is blocked from
  further crawling.
- **`LINKS_TO`** needs both companies to exist. Outbound links are remembered in
  Redis per target domain and replayed when that target is first resolved, so
  crawl order does not matter.
- **SSRF guard.** The dialer refuses private, loopback and link-local addresses
  unless `fetch.allow_private_networks` is set (tests and the demo set it).
- **Integration tests** sit behind the `integration` build tag because they need
  Docker. The same end-to-end and chaos scenarios also run without Docker
  against miniredis and in-memory stores, so `make test` covers them.
- **Extra commands** beyond the brief: `frontier`, `suppress`, `erase`, `kill`,
  `stats`, `rebuild`, `config`. `fetch` runs a dispatcher by default so the brief's five-stage
  layout still works without a separate frontier process.
