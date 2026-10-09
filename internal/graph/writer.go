package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/pkg/model"
)

var tracer = otel.Tracer("leadgraph/write")

// Writer is the graph-writer stage: it batches mutations from the write
// stream into transactions and acknowledges a message only after the
// transaction holding it has committed.
type Writer struct {
	cfg     config.Config
	streams *queue.Streams
	sink    Sink
	sup     *suppress.List
	log     *slog.Logger
	// OnPurge is called after a company purge so raw-store objects can be
	// removed too.
	OnPurge func(ctx context.Context, rep EraseReport)
}

func NewWriter(cfg config.Config, streams *queue.Streams, sink Sink, sup *suppress.List, log *slog.Logger) *Writer {
	if log == nil {
		log = slog.Default()
	}
	return &Writer{cfg: cfg, streams: streams, sink: sink, sup: sup, log: log.With("stage", "write")}
}

// Run consumes the write stream until ctx is cancelled.
func (w *Writer) Run(ctx context.Context) error {
	// Read enough messages to fill a transaction; a mutation is typically
	// tens of rows.
	readBatch := max(w.cfg.Queue.ReadBatch, w.cfg.Graph.BatchRows/20)
	c := queue.NewConsumer(w.streams, queue.ConsumerConfig{
		Stream: queue.Write, ReadBatch: readBatch, Block: w.cfg.Graph.BatchWait,
		ClaimMinIdle: w.cfg.Queue.ClaimMinIdle, MaxDeliveries: w.cfg.Queue.MaxDeliveries,
		DrainTimeout: w.cfg.Queue.DrainTimeout,
	}, w.log)
	return c.RunBatch(ctx, w.HandleBatch)
}

// Filter removes suppressed entities from a mutation: a suppressed domain
// drops everything, a suppressed email or person drops its node and every
// relationship touching it. It is the writer's last line of defence; the
// resolver applies the same list earlier.
func Filter(ctx context.Context, sup *suppress.List, mut *model.GraphMutation) (*model.GraphMutation, int, error) {
	if sup == nil {
		return mut, 0, nil
	}
	var emailKeys, emailIDs, personKeys, personNameKeys []string
	for _, n := range mut.Nodes {
		switch n.Label {
		case model.LabelCompany:
			if d, _ := n.Props["domain"].(string); d != "" && sup.DomainBlocked(ctx, d) {
				return &model.GraphMutation{SourceURL: mut.SourceURL, PurgeCompanies: mut.PurgeCompanies}, len(mut.Nodes), nil
			}
		case model.LabelEmail:
			emailKeys = append(emailKeys, n.Key)
			emailIDs = append(emailIDs, model.EmailID(n.Key))
		case model.LabelPerson:
			company, _ := n.Props["company_id"].(string)
			name, _ := n.Scalars["full_name"].Value.(string)
			personKeys = append(personKeys, n.Key)
			personNameKeys = append(personNameKeys, model.PersonNameKey(company, normalize.PersonKey(name)))
		}
	}
	if len(emailKeys) == 0 && len(personKeys) == 0 {
		return mut, 0, nil
	}
	drop := map[string]bool{}
	blocked, err := sup.HasMembers(ctx, suppress.KindEmail, emailIDs)
	if err != nil {
		return nil, 0, err
	}
	for i, b := range blocked {
		if b {
			drop[model.LabelEmail+"\x00"+emailKeys[i]] = true
		}
	}
	byID, err := sup.HasMembers(ctx, suppress.KindPerson, personKeys)
	if err != nil {
		return nil, 0, err
	}
	byName, err := sup.HasMembers(ctx, suppress.KindPersonName, personNameKeys)
	if err != nil {
		return nil, 0, err
	}
	for i := range personKeys {
		if byID[i] || byName[i] {
			drop[model.LabelPerson+"\x00"+personKeys[i]] = true
		}
	}
	if len(drop) == 0 {
		return mut, 0, nil
	}
	out := &model.GraphMutation{SourceURL: mut.SourceURL, PurgeCompanies: mut.PurgeCompanies}
	for _, n := range mut.Nodes {
		if !drop[n.Label+"\x00"+n.Key] {
			out.Nodes = append(out.Nodes, n)
		}
	}
	for _, e := range mut.Edges {
		if !drop[e.FromLabel+"\x00"+e.FromKey] && !drop[e.ToLabel+"\x00"+e.ToKey] {
			out.Edges = append(out.Edges, e)
		}
	}
	return out, len(drop), nil
}

// HandleBatch commits a batch. If the combined transaction fails, each
// mutation is retried alone so one bad message cannot poison its batch.
func (w *Writer) HandleBatch(ctx context.Context, msgs []queue.Message) []error {
	ctx, span := tracer.Start(ctx, "write-batch")
	span.SetAttributes(attribute.Int("messages", len(msgs)))
	defer span.End()

	errs := make([]error, len(msgs))
	muts := make([]*model.GraphMutation, len(msgs))
	var valid []*model.GraphMutation
	for i, m := range msgs {
		var mut model.GraphMutation
		if err := json.Unmarshal(m.Payload, &mut); err != nil {
			errs[i] = queue.Permanent(fmt.Errorf("write: decoding mutation: %w", err))
			continue
		}
		filtered, dropped, err := Filter(ctx, w.sup, &mut)
		if err != nil {
			errs[i] = err
			continue
		}
		if dropped > 0 {
			metrics.ResolveSuppressed.WithLabelValues("writer").Add(float64(dropped))
		}
		muts[i] = filtered
		valid = append(valid, filtered)
	}
	if len(valid) > 0 {
		if err := w.sink.Apply(ctx, valid); err != nil {
			w.log.Warn("batch commit failed; retrying mutations one by one", "messages", len(valid), "error", err)
			for i, mut := range muts {
				if mut == nil {
					continue
				}
				if aerr := w.sink.Apply(ctx, []*model.GraphMutation{mut}); aerr != nil {
					errs[i] = aerr
					muts[i] = nil
				}
			}
		}
	}
	// Purges run after the upserts of the same batch so the purge wins.
	for i, mut := range muts {
		if mut == nil {
			continue
		}
		for _, id := range mut.PurgeCompanies {
			rep, err := w.sink.PurgeCompany(ctx, id)
			if err != nil {
				errs[i] = err
				break
			}
			w.log.Info("company purged", "company_id", id, "nodes", rep.Nodes, "pages", len(rep.Pages))
			if w.OnPurge != nil {
				w.OnPurge(ctx, rep)
			}
		}
	}
	return errs
}
