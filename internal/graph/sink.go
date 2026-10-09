package graph

import (
	"context"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

// PageRef points at the stored artefacts of one page.
type PageRef struct {
	URL        string
	RawKey     string
	RawOffset  int64
	RawLength  int64
	ExtractKey string
}

// ErasedPerson describes a person removed from the graph.
type ErasedPerson struct {
	ID        string
	CompanyID string
	Name      string
}

// EraseReport lists what an erasure removed, so the caller can also remove
// the raw-store objects and add suppression entries.
type EraseReport struct {
	People []ErasedPerson
	Emails []string
	Pages  []PageRef
	// Nodes is the number of graph nodes deleted.
	Nodes int
}

func (r *EraseReport) merge(o EraseReport) {
	r.People = append(r.People, o.People...)
	r.Emails = append(r.Emails, o.Emails...)
	r.Pages = append(r.Pages, o.Pages...)
	r.Nodes += o.Nodes
}

// Stats is a count of nodes per label and relationships per type.
type Stats struct {
	Nodes map[string]int64
	Edges map[string]int64
}

// Sink is a graph store the writer can commit to.
type Sink interface {
	// Apply commits the mutations atomically per transaction and
	// idempotently overall: applying the same mutation twice changes nothing.
	Apply(ctx context.Context, muts []*model.GraphMutation) error
	// PurgeCompany removes a company and everything that only it owned.
	PurgeCompany(ctx context.Context, companyID string) (EraseReport, error)
}

// Eraser removes personal data on request.
type Eraser interface {
	// ErasePerson deletes a person, their relationships and the contact
	// nodes only they owned.
	ErasePerson(ctx context.Context, personID string) (EraseReport, error)
	// EraseEmail deletes an email address. With cascade, people who own it
	// as a personal address are erased as well.
	EraseEmail(ctx context.Context, address string, cascade bool) (EraseReport, error)
	// FlagSuppressed records a suppression entry in the graph itself.
	FlagSuppressed(ctx context.Context, kind, member string, at time.Time) error
	UnflagSuppressed(ctx context.Context, kind, member string) error
	// Suppressed lists the suppression entries recorded in the graph.
	Suppressed(ctx context.Context) (map[string][]string, error)
}

// Store is everything the CLI needs from a graph backend.
type Store interface {
	Sink
	Eraser
	Stats(ctx context.Context) (Stats, error)
	Close(ctx context.Context) error
}
