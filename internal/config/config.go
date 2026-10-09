// Package config loads configuration from defaults, an optional YAML file and
// environment variables (in that order of precedence, lowest first) and
// validates the result.
package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix is prepended to every environment override.
const EnvPrefix = "LEADGRAPH"

type Config struct {
	Redis     Redis     `yaml:"redis"`
	Neo4j     Neo4j     `yaml:"neo4j"`
	S3        S3        `yaml:"s3"`
	Crawl     Crawl     `yaml:"crawl"`
	Fetch     Fetch     `yaml:"fetch"`
	Extract   Extract   `yaml:"extract"`
	Parse     Parse     `yaml:"parse"`
	Resolve   Resolve   `yaml:"resolve"`
	Graph     Graph     `yaml:"graph"`
	Queue     Queue     `yaml:"queue"`
	Store     Store     `yaml:"store"`
	Telemetry Telemetry `yaml:"telemetry"`
}

type Redis struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// TLS enables TLS to Redis, as hosted Redis services require.
	TLS bool `yaml:"tls"`
	// KeyPrefix namespaces every key and stream.
	KeyPrefix string `yaml:"key_prefix"`
}

type Neo4j struct {
	URI      string `yaml:"uri"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type S3 struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type Crawl struct {
	// Epoch namespaces dedup and budgets; change it to recrawl everything.
	Epoch             string        `yaml:"epoch"`
	MaxDepth          int           `yaml:"max_depth"`
	MaxPagesPerDomain int           `yaml:"max_pages_per_domain"`
	DefaultDelay      time.Duration `yaml:"default_delay"`
	// MaxCrawlDelay caps a robots.txt Crawl-delay.
	MaxCrawlDelay time.Duration `yaml:"max_crawl_delay"`
	// GlobalRPS caps dispatched fetches per second across all processes.
	GlobalRPS        int           `yaml:"global_rps"`
	WellKnownPaths   []string      `yaml:"well_known_paths"`
	MaxQueuePerHost  int           `yaml:"max_queue_per_host"`
	MaxSitemapURLs   int           `yaml:"max_sitemap_urls"`
	MaxSitemaps      int           `yaml:"max_sitemaps_per_domain"`
	HostLease        time.Duration `yaml:"host_lease"`
	DispatchBatch    int           `yaml:"dispatch_batch"`
	DispatchInterval time.Duration `yaml:"dispatch_interval"`
}

type Headless struct {
	Enabled bool          `yaml:"enabled"`
	Workers int           `yaml:"workers"`
	Timeout time.Duration `yaml:"timeout"`
	// MinTextBytes: pages with less visible text than this (and at least one
	// script) are re-fetched with the headless browser.
	MinTextBytes int    `yaml:"min_text_bytes"`
	ExecPath     string `yaml:"exec_path"`
}

type Fetch struct {
	BotName    string `yaml:"bot_name"`
	BotVersion string `yaml:"bot_version"`
	// ContactURL is advertised in the User-Agent so site owners can reach us.
	ContactURL           string            `yaml:"contact_url"`
	Workers              int               `yaml:"workers"`
	DialTimeout          time.Duration     `yaml:"dial_timeout"`
	TLSTimeout           time.Duration     `yaml:"tls_timeout"`
	HeaderTimeout        time.Duration     `yaml:"header_timeout"`
	TotalTimeout         time.Duration     `yaml:"total_timeout"`
	MaxBodyBytes         int64             `yaml:"max_body_bytes"`
	MaxRedirects         int               `yaml:"max_redirects"`
	MaxRetries           int               `yaml:"max_retries"`
	BackoffBase          time.Duration     `yaml:"backoff_base"`
	BackoffMax           time.Duration     `yaml:"backoff_max"`
	RobotsTTL            time.Duration     `yaml:"robots_ttl"`
	BreakerFailures      int               `yaml:"breaker_failures"`
	BreakerCooldown      time.Duration     `yaml:"breaker_cooldown"`
	MaxRequeues          int               `yaml:"max_requeues"`
	MaxIdleConnsPerHost  int               `yaml:"max_idle_conns_per_host"`
	AllowPrivateNetworks bool              `yaml:"allow_private_networks"`
	HostOverrides        map[string]string `yaml:"host_overrides"`
	Proxies              []string          `yaml:"proxies"`
	ParseLagThreshold    int64             `yaml:"parse_lag_threshold"`
	Headless             Headless          `yaml:"headless"`
}

// UserAgent renders the identifying User-Agent header.
func (f Fetch) UserAgent() string {
	return fmt.Sprintf("%s/%s (+%s)", f.BotName, f.BotVersion, f.ContactURL)
}

type Extract struct {
	DefaultRegion     string `yaml:"default_region"`
	FallbackRegion    string `yaml:"fallback_region"`
	AllowReservedTLDs bool   `yaml:"allow_reserved_tlds"`
	TechRulesFile     string `yaml:"tech_rules_file"`
}

type Parse struct {
	Workers int `yaml:"workers"`
}

type Countries struct {
	Allow []string `yaml:"allow"`
	Deny  []string `yaml:"deny"`
	// Unknown is "allow" or "deny" for companies whose country is unknown
	// while an allow list is configured.
	Unknown string `yaml:"unknown"`
}

type Resolve struct {
	Workers          int       `yaml:"workers"`
	PersonSimilarity float64   `yaml:"person_similarity"`
	Countries        Countries `yaml:"countries"`
}

type Graph struct {
	BatchRows  int           `yaml:"batch_rows"`
	BatchWait  time.Duration `yaml:"batch_wait"`
	MaxFacts   int           `yaml:"max_facts"`
	MaxRetries int           `yaml:"max_retries"`
}

type Queue struct {
	// MaxLen is the hard cap on a stream's backlog; producers block at it.
	MaxLen        int64         `yaml:"max_len"`
	ClaimMinIdle  time.Duration `yaml:"claim_min_idle"`
	MaxDeliveries int           `yaml:"max_deliveries"`
	Block         time.Duration `yaml:"block"`
	ReadBatch     int           `yaml:"read_batch"`
	DrainTimeout  time.Duration `yaml:"drain_timeout"`
}

type Store struct {
	BatchRecords  int           `yaml:"batch_records"`
	BatchBytes    int           `yaml:"batch_bytes"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

type Telemetry struct {
	MetricsAddr  string `yaml:"metrics_addr"`
	OTLPEndpoint string `yaml:"otlp_endpoint"`
	ServiceName  string `yaml:"service_name"`
	LogLevel     string `yaml:"log_level"`
	LogFormat    string `yaml:"log_format"`
}

// Default returns the configuration with every limit at its default.
func Default() Config {
	return Config{
		Redis: Redis{Addr: "localhost:6379", KeyPrefix: "lg"},
		Neo4j: Neo4j{URI: "bolt://localhost:7687", Username: "neo4j", Database: "neo4j"},
		S3:    S3{Endpoint: "localhost:9000", Bucket: "leadgraph", Region: "us-east-1"},
		Crawl: Crawl{
			Epoch: "1", MaxDepth: 3, MaxPagesPerDomain: 40, DefaultDelay: time.Second,
			MaxCrawlDelay: 30 * time.Second, GlobalRPS: 50,
			WellKnownPaths:  []string{"/about", "/team", "/contact", "/people", "/leadership", "/company", "/careers", "/impressum"},
			MaxQueuePerHost: 2000, MaxSitemapURLs: 500, MaxSitemaps: 5, HostLease: 3 * time.Minute,
			DispatchBatch: 64, DispatchInterval: 100 * time.Millisecond,
		},
		Fetch: Fetch{
			BotName: "LeadGraphBot", BotVersion: "1.0", ContactURL: "https://example.com/bot",
			Workers: 32, DialTimeout: 5 * time.Second, TLSTimeout: 5 * time.Second,
			HeaderTimeout: 10 * time.Second, TotalTimeout: 30 * time.Second,
			MaxBodyBytes: 5 << 20, MaxRedirects: 5, MaxRetries: 3,
			BackoffBase: 500 * time.Millisecond, BackoffMax: 30 * time.Second,
			RobotsTTL: 24 * time.Hour, BreakerFailures: 5, BreakerCooldown: time.Minute,
			MaxRequeues: 3, MaxIdleConnsPerHost: 2, ParseLagThreshold: 5000,
			Headless: Headless{Workers: 2, Timeout: 20 * time.Second, MinTextBytes: 200},
		},
		Extract: Extract{FallbackRegion: "US"},
		Parse:   Parse{Workers: 8},
		Resolve: Resolve{Workers: 4, PersonSimilarity: 0.94, Countries: Countries{Unknown: "allow"}},
		Graph:   Graph{BatchRows: 1000, BatchWait: 500 * time.Millisecond, MaxFacts: 50, MaxRetries: 5},
		Queue: Queue{
			MaxLen: 10000, ClaimMinIdle: 5 * time.Minute, MaxDeliveries: 5,
			Block: 2 * time.Second, ReadBatch: 32, DrainTimeout: 30 * time.Second,
		},
		Store:     Store{BatchRecords: 200, BatchBytes: 16 << 20, FlushInterval: 2 * time.Second},
		Telemetry: Telemetry{MetricsAddr: ":9108", ServiceName: "leadgraph", LogLevel: "info", LogFormat: "json"},
	}
}

// Load builds the configuration from defaults, the YAML file at path (if
// non-empty) and the process environment, then validates it.
func Load(path string) (Config, error) {
	return LoadWith(path, os.Getenv)
}

// LoadWith is Load with an injectable environment lookup.
func LoadWith(path string, getenv func(string) string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && err.Error() != "EOF" {
			return cfg, fmt.Errorf("config: %s: %w", path, err)
		}
	}
	if err := applyEnv(reflect.ValueOf(&cfg).Elem(), EnvPrefix, getenv); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

var durationType = reflect.TypeOf(time.Duration(0))

// applyEnv overrides fields from variables named PREFIX_SECTION_FIELD, where
// the name parts are the upper-cased yaml tags.
func applyEnv(v reflect.Value, prefix string, getenv func(string) string) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		name := prefix + "_" + strings.ToUpper(tag)
		fv := v.Field(i)
		if fv.Kind() == reflect.Struct {
			if err := applyEnv(fv, name, getenv); err != nil {
				return err
			}
			continue
		}
		raw := getenv(name)
		if raw == "" {
			continue
		}
		if err := setField(fv, raw); err != nil {
			return fmt.Errorf("config: %s=%q: %w", name, raw, err)
		}
	}
	return nil
}

func setField(fv reflect.Value, raw string) error {
	switch {
	case fv.Type() == durationType:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return err
		}
		fv.SetInt(int64(d))
	case fv.Kind() == reflect.String:
		fv.SetString(raw)
	case fv.Kind() == reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case fv.Kind() == reflect.Int || fv.Kind() == reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case fv.Kind() == reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return err
		}
		fv.SetFloat(f)
	case fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() == reflect.String:
		var out []string
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		fv.Set(reflect.ValueOf(out))
	case fv.Kind() == reflect.Map:
		m := map[string]string{}
		for _, p := range strings.Split(raw, ",") {
			k, val, ok := strings.Cut(strings.TrimSpace(p), "=")
			if !ok {
				return fmt.Errorf("expected key=value, got %q", p)
			}
			m[k] = val
		}
		fv.Set(reflect.ValueOf(m))
	default:
		return fmt.Errorf("unsupported type %s", fv.Type())
	}
	return nil
}

// Validate checks every limit. It reports all problems at once.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	positive := func(name string, v int64) {
		if v <= 0 {
			bad("%s must be > 0 (got %d)", name, v)
		}
	}
	posDur := func(name string, d time.Duration) {
		if d <= 0 {
			bad("%s must be > 0 (got %s)", name, d)
		}
	}
	if c.Redis.Addr == "" {
		bad("redis.addr is required")
	}
	if c.Redis.KeyPrefix == "" || strings.ContainsAny(c.Redis.KeyPrefix, " \t{}") {
		bad("redis.key_prefix must be non-empty without spaces or braces")
	}
	if c.Crawl.Epoch == "" {
		bad("crawl.epoch is required")
	}
	if c.Crawl.MaxDepth < 0 || c.Crawl.MaxDepth > 20 {
		bad("crawl.max_depth must be in 0..20 (got %d)", c.Crawl.MaxDepth)
	}
	positive("crawl.max_pages_per_domain", int64(c.Crawl.MaxPagesPerDomain))
	positive("crawl.global_rps", int64(c.Crawl.GlobalRPS))
	positive("crawl.max_queue_per_host", int64(c.Crawl.MaxQueuePerHost))
	positive("crawl.dispatch_batch", int64(c.Crawl.DispatchBatch))
	posDur("crawl.dispatch_interval", c.Crawl.DispatchInterval)
	posDur("crawl.host_lease", c.Crawl.HostLease)
	if c.Crawl.DefaultDelay < 0 {
		bad("crawl.default_delay must be >= 0")
	}
	if c.Crawl.MaxCrawlDelay < c.Crawl.DefaultDelay {
		bad("crawl.max_crawl_delay must be >= crawl.default_delay")
	}
	for _, p := range c.Crawl.WellKnownPaths {
		if !strings.HasPrefix(p, "/") {
			bad("crawl.well_known_paths: %q must start with /", p)
		}
	}
	if c.Fetch.BotName == "" || strings.ContainsAny(c.Fetch.BotName, " /()") {
		bad("fetch.bot_name must be a single token")
	}
	if !strings.HasPrefix(c.Fetch.ContactURL, "http://") && !strings.HasPrefix(c.Fetch.ContactURL, "https://") {
		bad("fetch.contact_url must be an http(s) URL so site owners can identify the bot")
	}
	positive("fetch.workers", int64(c.Fetch.Workers))
	posDur("fetch.dial_timeout", c.Fetch.DialTimeout)
	posDur("fetch.tls_timeout", c.Fetch.TLSTimeout)
	posDur("fetch.header_timeout", c.Fetch.HeaderTimeout)
	posDur("fetch.total_timeout", c.Fetch.TotalTimeout)
	posDur("fetch.robots_ttl", c.Fetch.RobotsTTL)
	posDur("fetch.backoff_base", c.Fetch.BackoffBase)
	posDur("fetch.breaker_cooldown", c.Fetch.BreakerCooldown)
	positive("fetch.max_body_bytes", c.Fetch.MaxBodyBytes)
	positive("fetch.breaker_failures", int64(c.Fetch.BreakerFailures))
	positive("fetch.parse_lag_threshold", c.Fetch.ParseLagThreshold)
	if c.Fetch.MaxRedirects < 0 || c.Fetch.MaxRetries < 0 || c.Fetch.MaxRequeues < 0 {
		bad("fetch.max_redirects, fetch.max_retries and fetch.max_requeues must be >= 0")
	}
	if c.Fetch.BackoffMax < c.Fetch.BackoffBase {
		bad("fetch.backoff_max must be >= fetch.backoff_base")
	}
	if c.Fetch.Headless.Enabled {
		positive("fetch.headless.workers", int64(c.Fetch.Headless.Workers))
		posDur("fetch.headless.timeout", c.Fetch.Headless.Timeout)
	}
	if c.Crawl.HostLease < c.Fetch.TotalTimeout {
		bad("crawl.host_lease must be >= fetch.total_timeout, or a host could be fetched twice at once")
	}
	positive("parse.workers", int64(c.Parse.Workers))
	positive("resolve.workers", int64(c.Resolve.Workers))
	if c.Resolve.PersonSimilarity < 0.5 || c.Resolve.PersonSimilarity > 1 {
		bad("resolve.person_similarity must be in 0.5..1")
	}
	if u := c.Resolve.Countries.Unknown; u != "allow" && u != "deny" {
		bad("resolve.countries.unknown must be allow or deny")
	}
	for _, list := range [][]string{c.Resolve.Countries.Allow, c.Resolve.Countries.Deny} {
		for _, cc := range list {
			if len(cc) != 2 || strings.ToUpper(cc) != cc {
				bad("resolve.countries: %q is not an upper-case ISO alpha-2 code", cc)
			}
		}
	}
	if c.Graph.BatchRows < 1 || c.Graph.BatchRows > 10000 {
		bad("graph.batch_rows must be in 1..10000")
	}
	posDur("graph.batch_wait", c.Graph.BatchWait)
	positive("graph.max_facts", int64(c.Graph.MaxFacts))
	positive("queue.max_len", c.Queue.MaxLen)
	positive("queue.max_deliveries", int64(c.Queue.MaxDeliveries))
	positive("queue.read_batch", int64(c.Queue.ReadBatch))
	posDur("queue.claim_min_idle", c.Queue.ClaimMinIdle)
	posDur("queue.block", c.Queue.Block)
	positive("store.batch_records", int64(c.Store.BatchRecords))
	positive("store.batch_bytes", int64(c.Store.BatchBytes))
	posDur("store.flush_interval", c.Store.FlushInterval)
	switch c.Telemetry.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		bad("telemetry.log_level must be debug, info, warn or error")
	}
	if f := c.Telemetry.LogFormat; f != "json" && f != "text" {
		bad("telemetry.log_format must be json or text")
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
}

// RequireStores checks the credentials that have no defaults. Commands that
// talk to Neo4j or S3 call it; pure Redis commands do not need them.
func (c Config) RequireStores(neo4j, s3 bool) error {
	var errs []error
	if neo4j {
		if c.Neo4j.URI == "" || c.Neo4j.Username == "" {
			errs = append(errs, errors.New("neo4j.uri and neo4j.username are required"))
		}
		if c.Neo4j.Password == "" {
			errs = append(errs, errors.New("neo4j.password is required (set LEADGRAPH_NEO4J_PASSWORD)"))
		}
	}
	if s3 {
		if c.S3.Endpoint == "" || c.S3.Bucket == "" {
			errs = append(errs, errors.New("s3.endpoint and s3.bucket are required"))
		}
		if c.S3.AccessKey == "" || c.S3.SecretKey == "" {
			errs = append(errs, errors.New("s3.access_key and s3.secret_key are required (set LEADGRAPH_S3_ACCESS_KEY / LEADGRAPH_S3_SECRET_KEY)"))
		}
	}
	return errors.Join(errs...)
}
