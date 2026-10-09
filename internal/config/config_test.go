package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestDefaultsAreValid(t *testing.T) {
	cfg, err := LoadWith("", env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Crawl.MaxDepth != 3 || cfg.Crawl.MaxPagesPerDomain != 40 || cfg.Crawl.DefaultDelay != time.Second {
		t.Errorf("crawl defaults: %+v", cfg.Crawl)
	}
	if cfg.Fetch.MaxBodyBytes != 5<<20 || cfg.Fetch.MaxRedirects != 5 || cfg.Fetch.RobotsTTL != 24*time.Hour {
		t.Errorf("fetch defaults: %+v", cfg.Fetch)
	}
	if got := cfg.Fetch.UserAgent(); got != "LeadGraphBot/1.0 (+https://example.com/bot)" {
		t.Errorf("user agent = %q", got)
	}
	if cfg.Neo4j.Password != "" || cfg.S3.SecretKey != "" || cfg.Redis.Password != "" {
		t.Error("defaults must not contain secrets")
	}
	if err := cfg.RequireStores(true, true); err == nil {
		t.Error("RequireStores should fail without credentials")
	}
}

func TestYAMLAndEnvPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	yaml := `
crawl:
  max_depth: 2
  default_delay: 250ms
  well_known_paths: [/about, /kontakt]
fetch:
  contact_url: https://acme.example/bot
  host_overrides: {acme.test: "127.0.0.1:8080"}
  headless: {enabled: true, workers: 1}
resolve:
  countries: {deny: [RU, KP]}
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadWith(path, env(map[string]string{
		"LEADGRAPH_CRAWL_MAX_DEPTH":              "1",
		"LEADGRAPH_NEO4J_PASSWORD":               "s3cret",
		"LEADGRAPH_FETCH_TOTAL_TIMEOUT":          "12s",
		"LEADGRAPH_FETCH_ALLOW_PRIVATE_NETWORKS": "true",
		"LEADGRAPH_RESOLVE_COUNTRIES_ALLOW":      "US, DE",
		"LEADGRAPH_FETCH_HEADLESS_WORKERS":       "3",
		"LEADGRAPH_RESOLVE_PERSON_SIMILARITY":    "0.9",
		"LEADGRAPH_FETCH_HOST_OVERRIDES":         "a.test=127.0.0.1:1,b.test=127.0.0.1:2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Crawl.MaxDepth != 1 {
		t.Errorf("env should override yaml: depth=%d", cfg.Crawl.MaxDepth)
	}
	if cfg.Crawl.DefaultDelay != 250*time.Millisecond || len(cfg.Crawl.WellKnownPaths) != 2 {
		t.Errorf("yaml not applied: %+v", cfg.Crawl)
	}
	if cfg.Crawl.MaxPagesPerDomain != 40 {
		t.Error("unset fields should keep defaults")
	}
	if cfg.Neo4j.Password != "s3cret" || cfg.Fetch.TotalTimeout != 12*time.Second || !cfg.Fetch.AllowPrivateNetworks {
		t.Errorf("env not applied: %+v", cfg.Fetch)
	}
	if strings.Join(cfg.Resolve.Countries.Allow, ",") != "US,DE" || strings.Join(cfg.Resolve.Countries.Deny, ",") != "RU,KP" {
		t.Errorf("countries: %+v", cfg.Resolve.Countries)
	}
	if !cfg.Fetch.Headless.Enabled || cfg.Fetch.Headless.Workers != 3 || cfg.Resolve.PersonSimilarity != 0.9 {
		t.Errorf("nested env: %+v", cfg.Fetch.Headless)
	}
	if cfg.Fetch.HostOverrides["b.test"] != "127.0.0.1:2" || len(cfg.Fetch.HostOverrides) != 2 {
		t.Errorf("map env: %v", cfg.Fetch.HostOverrides)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"depth", map[string]string{"LEADGRAPH_CRAWL_MAX_DEPTH": "99"}, "crawl.max_depth"},
		{"pages", map[string]string{"LEADGRAPH_CRAWL_MAX_PAGES_PER_DOMAIN": "0"}, "crawl.max_pages_per_domain"},
		{"contact", map[string]string{"LEADGRAPH_FETCH_CONTACT_URL": "nope"}, "fetch.contact_url"},
		{"country", map[string]string{"LEADGRAPH_RESOLVE_COUNTRIES_DENY": "Germany"}, "ISO alpha-2"},
		{"unknown policy", map[string]string{"LEADGRAPH_RESOLVE_COUNTRIES_UNKNOWN": "maybe"}, "countries.unknown"},
		{"lease", map[string]string{"LEADGRAPH_CRAWL_HOST_LEASE": "1s"}, "crawl.host_lease"},
		{"log level", map[string]string{"LEADGRAPH_TELEMETRY_LOG_LEVEL": "loud"}, "log_level"},
		{"bad int", map[string]string{"LEADGRAPH_FETCH_WORKERS": "many"}, "LEADGRAPH_FETCH_WORKERS"},
		{"bad duration", map[string]string{"LEADGRAPH_FETCH_DIAL_TIMEOUT": "5"}, "LEADGRAPH_FETCH_DIAL_TIMEOUT"},
		{"batch rows", map[string]string{"LEADGRAPH_GRAPH_BATCH_ROWS": "50000"}, "graph.batch_rows"},
		{"prefix", map[string]string{"LEADGRAPH_REDIS_KEY_PREFIX": "a b"}, "redis.key_prefix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadWith("", env(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want mention of %q", err, tt.want)
			}
		})
	}
	// All problems are reported together.
	_, err := LoadWith("", env(map[string]string{"LEADGRAPH_CRAWL_MAX_DEPTH": "99", "LEADGRAPH_FETCH_WORKERS": "0"}))
	if err == nil || !strings.Contains(err.Error(), "max_depth") || !strings.Contains(err.Error(), "fetch.workers") {
		t.Errorf("expected both errors, got %v", err)
	}
}

func TestUnknownYAMLFieldRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("crawl:\n  max_dpeth: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadWith(path, env(nil)); err == nil {
		t.Error("typo in config key should be an error")
	}
	if _, err := LoadWith(filepath.Join(t.TempDir(), "missing.yaml"), env(nil)); err == nil {
		t.Error("missing file should be an error")
	}
}

// The documented example must stay loadable and in step with the defaults.
func TestExampleConfigMatchesDefaults(t *testing.T) {
	cfg, err := LoadWith(filepath.Join("..", "..", "config.example.yaml"), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Default()
	// The example spells out empty collections that the defaults leave nil.
	cfg.Fetch.HostOverrides, cfg.Fetch.Proxies = nil, nil
	cfg.Resolve.Countries.Allow, cfg.Resolve.Countries.Deny = nil, nil
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("config.example.yaml differs from Default():\n got %+v\nwant %+v", cfg, want)
	}
}
