// Command leadgraph is the single binary for every pipeline stage and for
// the operational commands around them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/extract"
	"github.com/appu/leadgraph/internal/fetch"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/obs"
	"github.com/appu/leadgraph/internal/pipeline"
	"github.com/appu/leadgraph/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type app struct {
	configPath string
	cfg        config.Config
	log        *slog.Logger
}

func root() *cobra.Command {
	a := &app{}
	cmd := &cobra.Command{
		Use:           "leadgraph",
		Short:         "Crawl public company websites into a provenance-carrying graph",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(a.configPath)
			if err != nil {
				return err
			}
			a.cfg = cfg
			a.log = obs.NewLogger(cfg.Telemetry, os.Stderr)
			return nil
		},
	}
	cmd.PersistentFlags().StringVarP(&a.configPath, "config", "c", os.Getenv("LEADGRAPH_CONFIG"), "path to a YAML config file (env: LEADGRAPH_CONFIG)")
	cmd.AddCommand(
		a.seedCmd(), a.frontierCmd(), a.fetchCmd(), a.parseCmd(), a.resolveCmd(), a.writeCmd(), a.runAllCmd(),
		a.migrateCmd(), a.dlqCmd(), a.suppressCmd(), a.eraseCmd(), a.killCmd(), a.statsCmd(), a.rebuildCmd(), a.configCmd(),
	)
	return cmd
}

// need selects which backing stores a command opens.
type need struct{ blob, graph bool }

// open builds a pipeline with the requested stores and returns a cleanup.
func (a *app) open(ctx context.Context, n need) (*pipeline.Pipeline, func(), error) {
	if err := a.cfg.RequireStores(n.graph, n.blob); err != nil {
		return nil, nil, err
	}
	rdb := pipeline.NewRedis(a.cfg.Redis)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, nil, fmt.Errorf("redis at %s: %w", a.cfg.Redis.Addr, err)
	}
	deps := pipeline.Deps{Cfg: a.cfg, RDB: rdb, Log: a.log}
	cleanup := []func(){func() { rdb.Close() }}
	closeAll := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}
	if a.cfg.Extract.TechRulesFile != "" {
		data, err := os.ReadFile(a.cfg.Extract.TechRulesFile)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		rules, err := extract.ParseTechRules(data)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		deps.Rules = rules
	}
	if n.blob {
		blob, err := store.NewS3(ctx, a.cfg.S3)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		deps.Blob = blob
	}
	if n.graph {
		g, err := graph.NewNeo4j(ctx, a.cfg.Neo4j, a.cfg.Graph, a.log)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		deps.Graph = g
		cleanup = append(cleanup, func() {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = g.Close(c)
		})
	}
	return pipeline.New(deps), closeAll, nil
}

// serve runs a long-lived stage with metrics, tracing and graceful shutdown.
func (a *app) serve(ctx context.Context, stage string, n need, headless bool, run func(context.Context, *pipeline.Pipeline) error) error {
	p, closeAll, err := a.open(ctx, n)
	if err != nil {
		return err
	}
	defer closeAll()
	shutdownTracing, err := obs.InitTracing(ctx, a.cfg.Telemetry, stage)
	if err != nil {
		a.log.Warn("tracing disabled", "error", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()
	if headless && a.cfg.Fetch.Headless.Enabled {
		r := fetch.NewChromeRenderer(a.cfg.Fetch.Headless, a.cfg.Fetch.UserAgent(), a.cfg.Fetch.HostOverrides)
		defer r.Close()
		p.Renderer = r
	}
	if n.graph {
		// Every process that writes makes sure the schema exists first.
		if neo, ok := p.Graph.(*graph.Neo4j); ok {
			if _, err := neo.Migrate(ctx); err != nil {
				return err
			}
		}
	}
	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	defer stopMetrics()
	go func() {
		err := metrics.Serve(metricsCtx, a.cfg.Telemetry.MetricsAddr, func(c context.Context) error { return p.RDB.Ping(c).Err() })
		if err != nil {
			a.log.Error("metrics server failed", "addr", a.cfg.Telemetry.MetricsAddr, "error", err)
		}
	}()
	a.log.Info("stage starting", "stage", stage, "version", version, "user_agent", a.cfg.Fetch.UserAgent())
	err = run(ctx, p)
	a.log.Info("stage stopped", "stage", stage)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
