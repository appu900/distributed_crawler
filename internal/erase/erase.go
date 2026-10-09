// Package erase removes a data subject from every store: the graph, the raw
// and extraction objects, the resolver's working state, and — by adding them
// to the suppression list first — from every future crawl.
package erase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/parse"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// Graph is what erasure needs from the graph store.
type Graph interface {
	graph.Eraser
	PurgeCompany(ctx context.Context, companyID string) (graph.EraseReport, error)
}

// Service performs erasure and suppression.
type Service struct {
	Graph    Graph
	Suppress *suppress.List
	Blob     store.Blob
	RDB      redis.UniversalClient
	Prefix   string
	Log      *slog.Logger
	Now      func() time.Time
}

// Result summarizes one erasure.
type Result struct {
	People      int
	Emails      int
	GraphNodes  int
	Pages       int
	RawRecords  int
	Extractions int
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// suppressMember adds a member to Redis and mirrors it into the graph, so
// the list can be rebuilt from either store.
func (s *Service) suppressMember(ctx context.Context, kind, member string) error {
	if err := s.Suppress.AddMember(ctx, kind, member); err != nil {
		return err
	}
	if s.Graph != nil {
		return s.Graph.FlagSuppressed(ctx, kind, member, s.now())
	}
	return nil
}

// Email erases an address. The address is suppressed before anything is
// deleted, so a pipeline that is running cannot write it back in between.
func (s *Service) Email(ctx context.Context, address string, cascade bool) (Result, error) {
	address = strings.ToLower(strings.TrimSpace(address))
	if !strings.Contains(address, "@") {
		return Result{}, fmt.Errorf("erase: %q is not an email address", address)
	}
	if err := s.suppressMember(ctx, suppress.KindEmail, model.EmailID(address)); err != nil {
		return Result{}, err
	}
	rep, err := s.Graph.EraseEmail(ctx, address, cascade)
	if err != nil {
		return Result{}, err
	}
	return s.finish(ctx, rep)
}

// Person erases a person by ID.
func (s *Service) Person(ctx context.Context, personID string) (Result, error) {
	personID = strings.TrimSpace(personID)
	if personID == "" {
		return Result{}, errors.New("erase: empty person id")
	}
	if err := s.suppressMember(ctx, suppress.KindPerson, personID); err != nil {
		return Result{}, err
	}
	rep, err := s.Graph.ErasePerson(ctx, personID)
	if err != nil {
		return Result{}, err
	}
	return s.finish(ctx, rep)
}

// Domain suppresses a domain and removes its company from the graph.
func (s *Service) Domain(ctx context.Context, domain string) (Result, error) {
	member, err := suppress.Member(suppress.KindDomain, domain)
	if err != nil {
		return Result{}, err
	}
	if err := s.suppressMember(ctx, suppress.KindDomain, member); err != nil {
		return Result{}, err
	}
	rep, err := s.Graph.PurgeCompany(ctx, model.CompanyID(member))
	if err != nil {
		return Result{}, err
	}
	if s.RDB != nil {
		id := model.CompanyID(member)
		s.RDB.Del(ctx, s.Prefix+":people:"+id, s.Prefix+":country:"+id)
	}
	return s.finish(ctx, rep)
}

// finish suppresses everything the graph reported as erased and removes the
// stored artefacts of the pages that mentioned it.
func (s *Service) finish(ctx context.Context, rep graph.EraseReport) (Result, error) {
	res := Result{People: len(rep.People), Emails: len(rep.Emails), GraphNodes: rep.Nodes}
	var errs []error
	for _, p := range rep.People {
		// The person ID depends on their title; the name key keeps them
		// out after a title change.
		errs = append(errs, s.suppressMember(ctx, suppress.KindPerson, p.ID))
		if p.CompanyID != "" && p.Name != "" {
			errs = append(errs, s.suppressMember(ctx, suppress.KindPersonName, suppress.PersonNameMember(p.CompanyID, p.Name)))
		}
		if s.RDB != nil && p.CompanyID != "" {
			errs = append(errs, s.RDB.HDel(ctx, s.Prefix+":people:"+p.CompanyID, p.ID).Err())
		}
	}
	for _, e := range rep.Emails {
		errs = append(errs, s.suppressMember(ctx, suppress.KindEmail, model.EmailID(e)))
	}
	raw, extractions, perr := s.CleanupPages(ctx, rep.Pages)
	errs = append(errs, perr)
	seen := map[string]bool{}
	for _, p := range rep.Pages {
		seen[p.URL] = true
	}
	res.Pages, res.RawRecords, res.Extractions = len(seen), raw, extractions
	return res, errors.Join(errs...)
}

// CleanupPages tombstones the raw records and deletes the extraction objects
// of the given pages, and forgets their "already parsed" markers.
func (s *Service) CleanupPages(ctx context.Context, pages []graph.PageRef) (raw, extractions int, err error) {
	var errs []error
	done := map[string]bool{}
	for _, p := range pages {
		if done[p.URL] {
			continue
		}
		done[p.URL] = true
		if s.Blob != nil && p.RawKey != "" && p.RawLength > 0 {
			terr := store.Tombstone(ctx, s.Blob, store.Ref{Key: p.RawKey, Offset: p.RawOffset, Length: p.RawLength})
			switch {
			case terr == nil:
				raw++
			case errors.Is(terr, store.ErrNotFound):
			default:
				errs = append(errs, fmt.Errorf("erase: raw record of %s: %w", p.URL, terr))
			}
		}
		if s.Blob != nil && p.ExtractKey != "" {
			if derr := s.Blob.Delete(ctx, p.ExtractKey); derr != nil {
				errs = append(errs, fmt.Errorf("erase: extraction of %s: %w", p.URL, derr))
			} else {
				extractions++
			}
		}
		if s.Blob != nil {
			// Older versions of the same page were extracted too.
			prefix := store.ExtractPrefix + urlx.Hash(p.URL)[:2] + "/" + urlx.Hash(p.URL) + "/"
			if keys, lerr := s.Blob.List(ctx, prefix); lerr == nil {
				for _, k := range keys {
					if s.Blob.Delete(ctx, k) == nil {
						extractions++
					}
				}
			}
		}
		if s.RDB != nil {
			s.RDB.Del(ctx, parse.ParsedKey(s.Prefix, p.URL))
		}
	}
	if len(done) > 0 {
		s.log().Info("stored page artefacts erased", "pages", len(done), "raw_records", raw, "extractions", extractions)
	}
	return raw, extractions, errors.Join(errs...)
}

// SuppressAdd adds an operator-supplied value to the suppression list and
// removes any matching data that is already stored.
func (s *Service) SuppressAdd(ctx context.Context, kind, value string) (Result, error) {
	switch kind {
	case suppress.KindEmail:
		return s.Email(ctx, value, true)
	case suppress.KindPerson:
		return s.Person(ctx, value)
	case suppress.KindDomain:
		return s.Domain(ctx, value)
	}
	return Result{}, fmt.Errorf("erase: cannot suppress kind %q directly", kind)
}

// SuppressRemove lifts a suppression in both stores.
func (s *Service) SuppressRemove(ctx context.Context, kind, value string) (bool, error) {
	member, err := suppress.Member(kind, value)
	if err != nil {
		return false, err
	}
	existed, err := s.Suppress.Remove(ctx, kind, value)
	if err != nil {
		return false, err
	}
	if s.Graph != nil {
		if err := s.Graph.UnflagSuppressed(ctx, kind, member); err != nil {
			return existed, err
		}
	}
	return existed, nil
}

// Sync copies suppression entries recorded in the graph back into Redis. Run
// it after Redis has been rebuilt from scratch.
func (s *Service) Sync(ctx context.Context) (int, error) {
	all, err := s.Graph.Suppressed(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for kind, members := range all {
		for _, m := range members {
			if err := s.Suppress.AddMember(ctx, kind, m); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}
