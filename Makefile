SHELL := /bin/sh
SEEDS ?= seeds.txt
CONFIG ?=
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Local credentials for docker-compose live in .env (created from
# .env.example on first `make up`). They are mapped to LEADGRAPH_* here so
# `make run` talks to the same containers.
-include .env
RUN_ENV = LEADGRAPH_NEO4J_PASSWORD='$(NEO4J_PASSWORD)' \
          LEADGRAPH_S3_ACCESS_KEY='$(MINIO_ROOT_USER)' \
          LEADGRAPH_S3_SECRET_KEY='$(MINIO_ROOT_PASSWORD)' \
          LEADGRAPH_FETCH_CONTACT_URL='$(LEADGRAPH_FETCH_CONTACT_URL)' \
          LEADGRAPH_TELEMETRY_LOG_FORMAT=text
CONFIG_FLAG = $(if $(CONFIG),--config $(CONFIG),)

.PHONY: help up down logs build test test-integration bench fuzz lint fmt vet run migrate stats demo demo-clean image clean

help: ## Show this help
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.env:
	cp .env.example .env
	@echo "created .env from .env.example - change the passwords before exposing these ports"

up: .env ## Start redis, neo4j, minio, prometheus and grafana, and wait until healthy
	docker compose up -d --wait redis neo4j minio
	docker compose up -d prometheus grafana

down: ## Stop the local infrastructure (keeps volumes)
	docker compose down

logs: ## Tail infrastructure logs
	docker compose logs -f --tail=50

build: ## Build bin/leadgraph
	CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/leadgraph ./cmd/leadgraph

vet: ## go vet (including the integration-tagged files)
	go vet ./...
	go vet -tags integration ./...

test: vet ## Unit and in-memory pipeline tests with the race detector (no Docker needed)
	go test -race -count=1 ./...

test-integration: ## Full-pipeline tests against real Redis, Neo4j and MinIO (testcontainers; needs Docker)
	go test -race -count=1 -tags integration -timeout 20m ./internal/integration/...

bench: ## Parser and graph-writer benchmarks
	go test -run '^$$' -bench . -benchmem ./internal/extract/ ./internal/graph/
	go test -run '^$$' -bench . -benchmem -tags integration -timeout 20m ./internal/integration/...

fuzz: ## Fuzz the URL normalizer and the HTML extractors for 30s each
	go test -run '^$$' -fuzz FuzzNormalize -fuzztime 30s ./internal/urlx/
	go test -run '^$$' -fuzz FuzzPage -fuzztime 30s ./internal/extract/

lint: vet ## golangci-lint (falls back to gofmt + vet when it is not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; \
	else echo "golangci-lint not installed; ran go vet only"; test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }; fi

fmt: ## Format the code
	gofmt -w .

migrate: ## Create graph constraints and indexes
	$(RUN_ENV) go run ./cmd/leadgraph $(CONFIG_FLAG) migrate

run: ## Crawl the domains in SEEDS (default seeds.txt) with every stage in one process
	@test -f $(SEEDS) || { echo "$(SEEDS) not found: cp seeds.example.txt seeds.txt and edit it"; exit 1; }
	$(RUN_ENV) go run ./cmd/leadgraph $(CONFIG_FLAG) run-all --seeds $(SEEDS)

stats: ## Print frontier, stream and graph statistics
	$(RUN_ENV) go run ./cmd/leadgraph $(CONFIG_FLAG) stats

demo: ## Crawl the bundled fixture sites and print graph statistics
	./scripts/demo.sh

image: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t leadgraph:$(VERSION) -t leadgraph:latest .

clean:
	rm -rf bin coverage.out

demo-clean: ## Remove the demo's graph data, Redis keys and bucket contents
	@set -a; . ./.env; set +a; \
	docker compose exec -T neo4j cypher-shell -u neo4j -p "$$NEO4J_PASSWORD" "MATCH (n) WHERE NOT n:SchemaMigration DETACH DELETE n" && \
	docker compose exec -T redis sh -c "redis-cli --scan --pattern 'lgdemo:*' | xargs -r redis-cli del >/dev/null" && \
	echo "demo data removed from Neo4j and Redis (objects remain in the leadgraph-demo bucket)"
