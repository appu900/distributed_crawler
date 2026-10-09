package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/appu/leadgraph/internal/dlq"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/parse"
	"github.com/appu/leadgraph/internal/pipeline"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/pkg/model"
)

func loadSeeds(file string, args []string) ([]string, error) {
	seeds := append([]string{}, args...)
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		fromFile, err := pipeline.ReadSeeds(f)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, fromFile...)
	}
	return seeds, nil
}

func (a *app) seedCmd() *cobra.Command {
	var file string
	var viaQueue bool
	cmd := &cobra.Command{
		Use:   "seed [domain...]",
		Short: "Add seed domains to the frontier (from arguments, a file, or via the Redis seed queue)",
		RunE: func(cmd *cobra.Command, args []string) error {
			seeds, err := loadSeeds(file, args)
			if err != nil {
				return err
			}
			if len(seeds) == 0 {
				return errors.New("no seeds given: pass domains as arguments or use --file")
			}
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			if viaQueue {
				vals := make([]any, len(seeds))
				for i, s := range seeds {
					vals[i] = s
				}
				if err := p.RDB.RPush(cmd.Context(), p.SeedQueueKey(), vals...).Err(); err != nil {
					return err
				}
				fmt.Printf("queued %d seeds on %s\n", len(seeds), p.SeedQueueKey())
				return nil
			}
			n, err := p.Seed(cmd.Context(), seeds)
			fmt.Printf("seeds: %d, urls enqueued: %d\n", len(seeds), n)
			return err
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "file with one seed domain or URL per line")
	cmd.Flags().BoolVar(&viaQueue, "queue", false, "push to the Redis seed queue instead of seeding directly")
	return cmd
}

func (a *app) frontierCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "frontier",
		Short: "Run the frontier dispatcher (moves due URLs to the fetch stream)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.serve(cmd.Context(), "frontier", need{}, false, func(ctx context.Context, p *pipeline.Pipeline) error {
				go p.RunLagCollector(ctx)
				return p.RunFrontier(ctx)
			})
		},
	}
}

func (a *app) fetchCmd() *cobra.Command {
	dispatch := true
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Run fetch workers (and, by default, a frontier dispatcher)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.serve(cmd.Context(), "fetch", need{blob: true}, true, func(ctx context.Context, p *pipeline.Pipeline) error {
				errc := make(chan error, 1)
				if dispatch {
					go func() { errc <- p.RunFrontier(ctx) }()
				} else {
					errc <- nil
				}
				err := p.RunFetch(ctx)
				return errors.Join(err, <-errc)
			})
		},
	}
	cmd.Flags().BoolVar(&dispatch, "dispatch", true, "also run a frontier dispatcher in this process")
	return cmd
}

func (a *app) parseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "parse",
		Short: "Run parse/extract workers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.serve(cmd.Context(), "parse", need{blob: true}, false, func(ctx context.Context, p *pipeline.Pipeline) error {
				return p.RunParse(ctx)
			})
		},
	}
}

func (a *app) resolveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resolve",
		Short: "Run entity-resolution workers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.serve(cmd.Context(), "resolve", need{}, false, func(ctx context.Context, p *pipeline.Pipeline) error {
				return p.RunResolve(ctx)
			})
		},
	}
}

func (a *app) writeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "write",
		Short: "Run the graph writer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.serve(cmd.Context(), "write", need{blob: true, graph: true}, false, func(ctx context.Context, p *pipeline.Pipeline) error {
				return p.RunWrite(ctx)
			})
		},
	}
}

func (a *app) runAllCmd() *cobra.Command {
	var file string
	var exitWhenIdle bool
	var idleFor time.Duration
	cmd := &cobra.Command{
		Use:   "run-all [domain...]",
		Short: "Run every stage in one process, optionally seeding first",
		RunE: func(cmd *cobra.Command, args []string) error {
			seeds, err := loadSeeds(file, args)
			if err != nil {
				return err
			}
			return a.serve(cmd.Context(), "run-all", need{blob: true, graph: true}, true, func(ctx context.Context, p *pipeline.Pipeline) error {
				if len(seeds) > 0 {
					n, err := p.Seed(ctx, seeds)
					a.log.Info("seeded", "seeds", len(seeds), "urls", n)
					if err != nil {
						a.log.Warn("some seeds were rejected", "error", err)
					}
				}
				if !exitWhenIdle {
					return p.RunAll(ctx)
				}
				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- p.RunAll(runCtx) }()
				werr := p.WaitIdle(runCtx, idleFor)
				if werr == nil {
					a.log.Info("pipeline idle; shutting down")
				}
				cancel()
				return <-done
			})
		},
	}
	cmd.Flags().StringVarP(&file, "seeds", "s", "", "seed file to load before starting")
	cmd.Flags().BoolVar(&exitWhenIdle, "exit-when-idle", false, "stop once the frontier and all streams are empty")
	cmd.Flags().DurationVar(&idleFor, "idle-for", 3*time.Second, "how long the pipeline must stay idle before exiting")
	return cmd
}

func (a *app) migrateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Create graph constraints and indexes (versioned, idempotent)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{graph: true})
			if err != nil {
				return err
			}
			defer closeAll()
			applied, err := p.Graph.(*graph.Neo4j).Migrate(cmd.Context())
			if err != nil {
				return err
			}
			if len(applied) == 0 {
				fmt.Printf("schema is up to date (version %d)\n", graph.Migrations[len(graph.Migrations)-1].Version)
			} else {
				fmt.Printf("applied migrations: %v\n", applied)
			}
			return nil
		},
	}
}

func (a *app) dlqCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "dlq", Short: "Inspect, replay or purge the dead-letter stream"}
	var limit int
	var stream string
	list := &cobra.Command{
		Use:   "list",
		Short: "Show dead-lettered messages",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			entries, err := dlq.List(cmd.Context(), p.Streams, int64(limit))
			if err != nil {
				return err
			}
			size, _ := dlq.Size(cmd.Context(), p.Streams)
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tSTREAM\tDELIVERIES\tFAILED AT\tERROR\tPAYLOAD")
			for _, e := range entries {
				payload := string(e.Payload)
				if len(payload) > 80 {
					payload = payload[:80] + "…"
				}
				fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n", e.ID, e.Stream, e.Deliveries, e.FailedAt, e.Error, payload)
			}
			w.Flush()
			fmt.Printf("%d of %d dead letters shown\n", len(entries), size)
			return nil
		},
	}
	list.Flags().IntVar(&limit, "limit", 50, "maximum entries to show")
	replay := &cobra.Command{
		Use:   "replay",
		Short: "Move dead letters back to the stream they failed in",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			n, err := dlq.Replay(cmd.Context(), p.Streams, stream, limit)
			fmt.Printf("replayed %d messages\n", n)
			return err
		},
	}
	replay.Flags().IntVar(&limit, "limit", 0, "maximum messages to replay (0 = all)")
	replay.Flags().StringVar(&stream, "stream", "", "only replay messages that failed in this stream (fetch|parse|resolve|write)")
	purge := &cobra.Command{
		Use:   "purge",
		Short: "Delete dead letters",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			n, err := dlq.Purge(cmd.Context(), p.Streams, stream)
			fmt.Printf("purged %d messages\n", n)
			return err
		},
	}
	purge.Flags().StringVar(&stream, "stream", "", "only purge messages that failed in this stream")
	cmd.AddCommand(list, replay, purge)
	return cmd
}

func printResult(what string, r interface {
	String() string
}) {
	fmt.Printf("%s: %s\n", what, r.String())
}

type eraseSummary struct {
	People, Emails, GraphNodes, Pages, RawRecords, Extractions int
}

func (e eraseSummary) String() string {
	return fmt.Sprintf("people=%d emails=%d graph_nodes=%d pages=%d raw_records_tombstoned=%d extractions_deleted=%d",
		e.People, e.Emails, e.GraphNodes, e.Pages, e.RawRecords, e.Extractions)
}

func (a *app) suppressCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "suppress", Short: "Manage the suppression (do-not-store) list"}
	var email, personID, domain string
	pick := func() (kind, value string, err error) {
		set := 0
		for k, v := range map[string]string{suppress.KindEmail: email, suppress.KindPerson: personID, suppress.KindDomain: domain} {
			if v != "" {
				kind, value = k, v
				set++
			}
		}
		if set != 1 {
			return "", "", errors.New("give exactly one of --email, --person-id, --domain")
		}
		return kind, value, nil
	}
	flags := func(c *cobra.Command) {
		c.Flags().StringVar(&email, "email", "", "email address")
		c.Flags().StringVar(&personID, "person-id", "", "person ID")
		c.Flags().StringVar(&domain, "domain", "", "registrable domain")
	}
	add := &cobra.Command{
		Use:   "add",
		Short: "Suppress an email, person or domain and remove any stored data for it",
		RunE: func(cmd *cobra.Command, _ []string) error {
			kind, value, err := pick()
			if err != nil {
				return err
			}
			p, closeAll, err := a.open(cmd.Context(), need{blob: true, graph: true})
			if err != nil {
				return err
			}
			defer closeAll()
			res, err := p.Erase.SuppressAdd(cmd.Context(), kind, value)
			printResult("suppressed "+kind, eraseSummary(res))
			return err
		},
	}
	flags(add)
	remove := &cobra.Command{
		Use:   "remove",
		Short: "Lift a suppression (data is not restored; it returns on the next crawl)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			kind, value, err := pick()
			if err != nil {
				return err
			}
			p, closeAll, err := a.open(cmd.Context(), need{graph: true})
			if err != nil {
				return err
			}
			defer closeAll()
			existed, err := p.Erase.SuppressRemove(cmd.Context(), kind, value)
			if err != nil {
				return err
			}
			if !existed {
				fmt.Println("no such suppression entry")
				return nil
			}
			fmt.Println("suppression removed")
			return nil
		},
	}
	flags(remove)
	list := &cobra.Command{
		Use:   "list",
		Short: "Show suppression list sizes and suppressed domains (emails and people are stored as hashes)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			for _, kind := range append(append([]string{}, suppress.Kinds...), suppress.KindGeo) {
				n, err := p.Suppress.Count(cmd.Context(), kind)
				if err != nil {
					return err
				}
				fmt.Printf("%-12s %d\n", kind, n)
			}
			for _, kind := range []string{suppress.KindDomain, suppress.KindGeo} {
				members, _ := p.Suppress.Members(cmd.Context(), kind)
				sort.Strings(members)
				for _, m := range members {
					fmt.Printf("  %s: %s\n", kind, m)
				}
			}
			return nil
		},
	}
	sync := &cobra.Command{
		Use:   "sync",
		Short: "Rebuild the Redis suppression sets from the flags stored in the graph",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{graph: true})
			if err != nil {
				return err
			}
			defer closeAll()
			n, err := p.Erase.Sync(cmd.Context())
			fmt.Printf("synced %d entries\n", n)
			return err
		},
	}
	cmd.AddCommand(add, remove, list, sync)
	return cmd
}

func (a *app) eraseCmd() *cobra.Command {
	var email, personID string
	var noCascade bool
	cmd := &cobra.Command{
		Use:   "erase",
		Short: "Erase a person or email everywhere (graph, raw store, extractions) and suppress it",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if (email == "") == (personID == "") {
				return errors.New("give exactly one of --email or --person-id")
			}
			p, closeAll, err := a.open(cmd.Context(), need{blob: true, graph: true})
			if err != nil {
				return err
			}
			defer closeAll()
			if email != "" {
				res, err := p.Erase.Email(cmd.Context(), email, !noCascade)
				printResult("erased email", eraseSummary(res))
				return err
			}
			res, err := p.Erase.Person(cmd.Context(), personID)
			printResult("erased person", eraseSummary(res))
			return err
		},
	}
	cmd.Flags().StringVar(&email, "email", "", "email address to erase")
	cmd.Flags().StringVar(&personID, "person-id", "", "person ID to erase")
	cmd.Flags().BoolVar(&noCascade, "no-cascade", false, "with --email: keep the person who owns a personal address")
	return cmd
}

func (a *app) killCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "kill", Short: "Control the kill switch that halts all fetching"}
	var reason string
	on := &cobra.Command{
		Use: "on", Short: "Halt all fetching, in every process, within a second",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			if err := p.Frontier.SetKill(cmd.Context(), true, reason); err != nil {
				return err
			}
			fmt.Println("kill switch ON: no URL will be dispatched or fetched")
			return nil
		},
	}
	on.Flags().StringVar(&reason, "reason", "", "why fetching is being halted")
	off := &cobra.Command{
		Use: "off", Short: "Resume fetching",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			if err := p.Frontier.SetKill(cmd.Context(), false, ""); err != nil {
				return err
			}
			fmt.Println("kill switch OFF")
			return nil
		},
	}
	status := &cobra.Command{
		Use: "status", Short: "Show whether the kill switch is on",
		RunE: func(cmd *cobra.Command, _ []string) error {
			p, closeAll, err := a.open(cmd.Context(), need{})
			if err != nil {
				return err
			}
			defer closeAll()
			killed, why, err := p.Frontier.Killed(cmd.Context())
			if err != nil {
				return err
			}
			if killed {
				fmt.Printf("ON (%s)\n", why)
			} else {
				fmt.Println("OFF")
			}
			return nil
		},
	}
	cmd.AddCommand(on, off, status)
	return cmd
}

func (a *app) statsCmd() *cobra.Command {
	var noGraph bool
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Print frontier, stream and graph statistics",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			p, closeAll, err := a.open(ctx, need{graph: !noGraph})
			if err != nil {
				return err
			}
			defer closeAll()
			fs, err := p.Frontier.Stats(ctx)
			if err != nil {
				return err
			}
			killed, _, _ := p.Frontier.Killed(ctx)
			fmt.Printf("Frontier\n  hosts: %d\n  queued urls: %d\n  kill switch: %v\n", fs.Hosts, fs.Queued, killed)
			lag, err := p.Lag(ctx)
			if err != nil {
				return err
			}
			fmt.Println("Streams (backlog)")
			for _, st := range append(append([]string{}, queue.Stages...), queue.DLQ) {
				fmt.Printf("  %-8s %d\n", st, lag[st])
			}
			if noGraph {
				return nil
			}
			gs, err := p.Graph.Stats(ctx)
			if err != nil {
				return err
			}
			printCounts := func(title string, m map[string]int64) {
				fmt.Println(title)
				keys := make([]string, 0, len(m))
				for k := range m {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					fmt.Printf("  %-16s %d\n", k, m[k])
				}
			}
			printCounts("Graph nodes", gs.Nodes)
			printCounts("Graph relationships", gs.Edges)
			return nil
		},
	}
	cmd.Flags().BoolVar(&noGraph, "no-graph", false, "skip graph statistics (no Neo4j connection needed)")
	return cmd
}

// rebuildCmd re-publishes every stored raw record to the parse stream, so
// the graph can be regenerated from the raw store alone.
func (a *app) rebuildCmd() *cobra.Command {
	var prefix string
	cmd := &cobra.Command{
		Use:   "rebuild",
		Short: "Re-parse stored raw pages (parse, resolve and write workers must be running)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			p, closeAll, err := a.open(ctx, need{blob: true})
			if err != nil {
				return err
			}
			defer closeAll()
			keys, err := p.Blob.List(ctx, store.RawPrefix+strings.TrimPrefix(prefix, store.RawPrefix))
			if err != nil {
				return err
			}
			published, skipped := 0, 0
			for _, key := range keys {
				err := store.ScanBatch(ctx, p.Blob, key, func(ref store.Ref, rec *model.RawRecord) error {
					if rec.Tombstone || rec.Kind == model.KindSitemap {
						skipped++
						return nil
					}
					pageURL := rec.FinalURL
					if pageURL == "" {
						pageURL = rec.URL
					}
					// Forget the "already parsed" marker so the page is extracted again.
					p.RDB.Del(ctx, parse.ParsedKey(a.cfg.Redis.KeyPrefix, pageURL))
					payload, err := json.Marshal(model.FetchResult{
						URL: rec.URL, FinalURL: rec.FinalURL, Kind: model.KindPage, Depth: a.cfg.Crawl.MaxDepth,
						Status: rec.Status, S3Key: ref.Key, RawOffset: ref.Offset, RawLength: ref.Length,
						FetchedAt: rec.FetchedAt, ContentHash: rec.ContentHash, Rendered: rec.Rendered,
					})
					if err != nil {
						return err
					}
					published++
					return p.Streams.Publish(ctx, queue.Parse, payload)
				})
				if err != nil {
					return fmt.Errorf("rebuild: %s: %w", key, err)
				}
			}
			fmt.Printf("batches: %d, records re-queued: %d, skipped (erased or sitemap): %d\n", len(keys), published, skipped)
			return nil
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "only rebuild raw objects under this key prefix (e.g. 2026/10/09)")
	return cmd
}

func (a *app) configCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "config",
		Short: "Validate the configuration and print it with secrets redacted",
		RunE: func(*cobra.Command, []string) error {
			c := a.cfg
			redact := func(s *string) {
				if *s != "" {
					*s = "<redacted>"
				}
			}
			redact(&c.Redis.Password)
			redact(&c.Neo4j.Password)
			redact(&c.S3.AccessKey)
			redact(&c.S3.SecretKey)
			out, err := yaml.Marshal(c)
			if err != nil {
				return err
			}
			fmt.Print(string(out))
			return nil
		},
	}
}
