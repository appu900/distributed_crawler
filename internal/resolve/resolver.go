package resolve

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

var tracer = otel.Tracer("leadgraph/resolve")

// maxInboundLinks bounds the remembered linkers per target domain.
const maxInboundLinks = 2000

// Resolver is the entity-resolution stage.
type Resolver struct {
	cfg      config.Config
	rdb      redis.UniversalClient
	prefix   string
	streams  *queue.Streams
	suppress *suppress.List
	policy   Policy
	log      *slog.Logger
}

func New(cfg config.Config, rdb redis.UniversalClient, streams *queue.Streams, sup *suppress.List, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	return &Resolver{cfg: cfg, rdb: rdb, prefix: cfg.Redis.KeyPrefix, streams: streams, suppress: sup,
		policy: NewPolicy(cfg.Resolve.Countries), log: log.With("stage", "resolve")}
}

// Run consumes the resolve stream until ctx is cancelled.
func (r *Resolver) Run(ctx context.Context) error {
	c := queue.NewConsumer(r.streams, queue.ConsumerConfig{
		Stream: queue.Resolve, Concurrency: r.cfg.Resolve.Workers, ReadBatch: r.cfg.Queue.ReadBatch,
		Block: r.cfg.Queue.Block, ClaimMinIdle: r.cfg.Queue.ClaimMinIdle,
		MaxDeliveries: r.cfg.Queue.MaxDeliveries, DrainTimeout: r.cfg.Queue.DrainTimeout,
	}, r.log)
	return c.Run(ctx, r.Handle)
}

// Handle resolves one extraction and publishes its mutation.
func (r *Resolver) Handle(ctx context.Context, m queue.Message) error {
	var ex model.Extraction
	if err := json.Unmarshal(m.Payload, &ex); err != nil {
		return queue.Permanent(fmt.Errorf("resolve: decoding extraction: %w", err))
	}
	if ex.URL == "" || ex.Domain == "" {
		return queue.Permanent(errors.New("resolve: extraction without url or domain"))
	}
	ctx, span := tracer.Start(ctx, "resolve")
	span.SetAttributes(attribute.String("url", ex.URL))
	defer span.End()

	mut, err := r.Resolve(ctx, &ex)
	if err != nil {
		return err
	}
	if mut == nil {
		return nil
	}
	payload, err := json.Marshal(mut)
	if err != nil {
		return queue.Permanent(err)
	}
	metrics.ResolveMutations.WithLabelValues("node").Add(float64(len(mut.Nodes)))
	metrics.ResolveMutations.WithLabelValues("edge").Add(float64(len(mut.Edges)))
	return r.streams.Publish(ctx, queue.Write, payload)
}

// personEntry is what the per-company registry remembers about a person.
type personEntry struct {
	Name     string `json:"n"`
	LinkedIn string `json:"l,omitempty"`
}

func (r *Resolver) peopleKey(companyID string) string  { return r.prefix + ":people:" + companyID }
func (r *Resolver) countryKey(companyID string) string { return r.prefix + ":country:" + companyID }
func (r *Resolver) linksKey(domain string) string      { return r.prefix + ":links:in:" + domain }

// lock serializes resolution per company so two workers cannot register the
// same person under two IDs.
func (r *Resolver) lock(ctx context.Context, companyID string) (unlock func(), err error) {
	key := r.prefix + ":lock:company:" + companyID
	var tok [8]byte
	_, _ = rand.Read(tok[:])
	token := hex.EncodeToString(tok[:])
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok, err := r.rdb.SetNX(ctx, key, token, 30*time.Second).Result()
		if err != nil {
			return nil, err
		}
		if ok {
			return func() {
				// Release only our own lock.
				c := context.WithoutCancel(ctx)
				if v, err := r.rdb.Get(c, key).Result(); err == nil && v == token {
					r.rdb.Del(c, key)
				}
			}, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("resolve: company %s is locked by another worker", companyID)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type builder struct {
	mut   *model.GraphMutation
	seen  time.Time
	nodes map[string]int
	edges map[string]int
}

func (b *builder) node(n model.NodeUpsert) {
	n.SeenAt = b.seen
	key := n.Label + "\x00" + n.Key
	if i, ok := b.nodes[key]; ok {
		prev := &b.mut.Nodes[i]
		for k, v := range n.Props {
			if prev.Props == nil {
				prev.Props = map[string]any{}
			}
			prev.Props[k] = v
		}
		for k, v := range n.Scalars {
			if prev.Scalars == nil {
				prev.Scalars = map[string]model.Scalar{}
			}
			if Better(v, prev.Scalars[k]) {
				prev.Scalars[k] = v
			}
		}
		prev.Facts = append(prev.Facts, n.Facts...)
		return
	}
	b.nodes[key] = len(b.mut.Nodes)
	b.mut.Nodes = append(b.mut.Nodes, n)
}

func (b *builder) edge(e model.EdgeUpsert) {
	e.SeenAt = b.seen
	if e.SourceURL == "" {
		e.SourceURL = b.mut.SourceURL
	}
	key := e.Type + "\x00" + e.FromLabel + "\x00" + e.FromKey + "\x00" + e.ToLabel + "\x00" + e.ToKey
	if i, ok := b.edges[key]; ok {
		if e.Confidence > b.mut.Edges[i].Confidence {
			b.mut.Edges[i] = e
		}
		return
	}
	b.edges[key] = len(b.mut.Edges)
	b.mut.Edges = append(b.mut.Edges, e)
}

// Resolve builds the mutation for one extraction. A nil mutation means there
// is nothing to write.
func (r *Resolver) Resolve(ctx context.Context, ex *model.Extraction) (*model.GraphMutation, error) {
	if r.suppress.DomainBlocked(ctx, ex.Domain) {
		metrics.ResolveSuppressed.WithLabelValues("domain").Inc()
		return nil, nil
	}
	failed := ex.SkipReason != "" || ex.Status < 200 || ex.Status >= 300
	if ex.Probe && failed {
		// A guessed well-known path that does not exist is not a page.
		return nil, nil
	}
	companyID := model.CompanyID(ex.Domain)
	b := &builder{mut: &model.GraphMutation{SourceURL: ex.URL}, seen: ex.FetchedAt, nodes: map[string]int{}, edges: map[string]int{}}

	// Company, domain and page always exist for a visited URL.
	b.node(model.NodeUpsert{Label: model.LabelCompany, Key: companyID, Props: map[string]any{"domain": ex.Domain}})
	b.node(model.NodeUpsert{Label: model.LabelDomain, Key: ex.Host, Props: map[string]any{
		"registrable": ex.Domain, "tld": urlx.TLD(ex.Host),
	}})
	b.edge(model.EdgeUpsert{Type: model.RelHasDomain, FromLabel: model.LabelCompany, FromKey: companyID,
		ToLabel: model.LabelDomain, ToKey: ex.Host, Confidence: 1})
	pageScalar := func(v any) model.Scalar { return model.Scalar{Value: v, Confidence: 1, At: ex.FetchedAt} }
	page := map[string]model.Scalar{
		"status": pageScalar(ex.Status), "fetched_at": pageScalar(ex.FetchedAt.UTC().Format(time.RFC3339Nano)),
		"skip_reason": pageScalar(ex.SkipReason),
	}
	if !ex.Unchanged {
		page["type"] = pageScalar(string(ex.PageType))
		if !failed {
			page["content_hash"] = pageScalar(ex.ContentHash)
			page["title"] = pageScalar(ex.Title)
			page["raw_key"] = pageScalar(ex.RawKey)
			page["raw_offset"] = pageScalar(ex.RawOffset)
			page["raw_length"] = pageScalar(ex.RawLength)
			page["extract_key"] = pageScalar(ex.ExtractKey)
		}
	}
	b.node(model.NodeUpsert{Label: model.LabelPage, Key: ex.URL, Scalars: page})
	b.edge(model.EdgeUpsert{Type: model.RelHasPage, FromLabel: model.LabelDomain, FromKey: ex.Host,
		ToLabel: model.LabelPage, ToKey: ex.URL, Confidence: 1})
	if failed || ex.Unchanged || ex.NoIndex {
		return b.mut, nil
	}

	// Country policy.
	country, strength, err := r.companyCountry(ctx, companyID, ex)
	if err != nil {
		return nil, err
	}
	if !r.policy.Allowed(country) {
		metrics.ResolveGeoDropped.WithLabelValues().Inc()
		r.log.Info("company dropped by country policy", "domain", ex.Domain, "country", country)
		if country != "" {
			// A known, disallowed country: stop crawling the domain and
			// remove anything stored before the country was known.
			if err := r.suppress.AddMember(ctx, suppress.KindGeo, ex.Domain); err != nil {
				return nil, err
			}
			return &model.GraphMutation{SourceURL: ex.URL, PurgeCompanies: []string{companyID}}, nil
		}
		return nil, nil
	}

	unlock, err := r.lock(ctx, companyID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Company scalars and provenance.
	company := model.NodeUpsert{Label: model.LabelCompany, Key: companyID, Scalars: map[string]model.Scalar{}}
	for _, cf := range companyFields(ex) {
		if best, ok := Best(cf.facts, cf.clean); ok {
			company.Scalars[cf.field] = scalar(best)
			if len(cf.facts) > 1 {
				metrics.ResolveMerges.WithLabelValues("scalar").Add(float64(len(cf.facts) - 1))
			}
		}
		for _, f := range cf.facts {
			if cf.clean != nil {
				f.Value = cf.clean(f.Value)
			}
			if f.Value != "" {
				company.Facts = append(company.Facts, FactString(cf.field, f))
			}
		}
	}
	if country != "" {
		company.Scalars["country"] = model.Scalar{Value: country, Confidence: []float64{0, 0.3, 0.5, 0.9}[strength], At: ex.FetchedAt}
	}
	b.node(company)

	companyEdge := func(typ, toLabel, toKey string, f model.Fact) {
		b.edge(model.EdgeUpsert{Type: typ, FromLabel: model.LabelCompany, FromKey: companyID, ToLabel: toLabel,
			ToKey: toKey, SourceURL: f.SourceURL, Confidence: f.Confidence})
	}
	mention := func(toLabel, toKey string, conf float64) {
		b.edge(model.EdgeUpsert{Type: model.RelMentions, FromLabel: model.LabelPage, FromKey: ex.URL,
			ToLabel: toLabel, ToKey: toKey, Confidence: conf})
	}

	// Emails, minus suppressed ones.
	emails := map[string]model.EmailFact{}
	if len(ex.Emails) > 0 {
		ids := make([]string, len(ex.Emails))
		for i, e := range ex.Emails {
			ids[i] = model.EmailID(e.Value)
		}
		blocked, err := r.suppress.HasMembers(ctx, suppress.KindEmail, ids)
		if err != nil {
			return nil, err
		}
		for i, e := range ex.Emails {
			if blocked[i] {
				metrics.ResolveSuppressed.WithLabelValues("email").Inc()
				continue
			}
			emails[e.Value] = e
			b.node(model.NodeUpsert{Label: model.LabelEmail, Key: e.Value, Props: map[string]any{
				"id": ids[i], "kind": e.Kind, "status": "unverified", "domain": e.Domain, "disposable": e.Disposable,
			}})
			mention(model.LabelEmail, e.Value, e.Confidence)
		}
	}
	phones := map[string]model.PhoneFact{}
	for _, p := range ex.Phones {
		phones[p.Value] = p
		b.node(model.NodeUpsert{Label: model.LabelPhone, Key: p.Value, Props: map[string]any{"kind": p.Kind}})
		mention(model.LabelPhone, p.Value, p.Confidence)
	}

	// People.
	registry, err := r.loadPeople(ctx, companyID)
	if err != nil {
		return nil, err
	}
	ownedEmails, ownedPhones := map[string]bool{}, map[string]bool{}
	for _, pf := range ex.People {
		personID, ok, err := r.resolvePerson(ctx, companyID, pf, registry)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		name := normalize.PersonName(pf.Value)
		title := normalize.Title(pf.Title)
		node := model.NodeUpsert{Label: model.LabelPerson, Key: personID, Props: map[string]any{"company_id": companyID},
			Scalars: map[string]model.Scalar{"full_name": {Value: name, Confidence: pf.Confidence, At: pf.ExtractedAt}}}
		nameFact := pf.Fact
		nameFact.Value = name
		node.Facts = append(node.Facts, FactString("full_name", nameFact))
		edgeProps := map[string]any{}
		if title != "" {
			node.Scalars["title"] = model.Scalar{Value: title, Confidence: pf.Confidence, At: pf.ExtractedAt}
			node.Scalars["seniority"] = model.Scalar{Value: normalize.Seniority(title), Confidence: pf.Confidence, At: pf.ExtractedAt}
			titleFact := pf.Fact
			titleFact.Value = title
			node.Facts = append(node.Facts, FactString("title", titleFact))
			edgeProps["title"] = title
		}
		b.node(node)
		b.edge(model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: companyID,
			ToLabel: model.LabelPerson, ToKey: personID, Props: edgeProps, SourceURL: pf.SourceURL, Confidence: pf.Confidence})
		mention(model.LabelPerson, personID, pf.Confidence)
		for _, addr := range pf.Emails {
			if e, ok := emails[addr]; ok {
				ownedEmails[addr] = true
				b.edge(model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelPerson, FromKey: personID,
					ToLabel: model.LabelEmail, ToKey: addr, SourceURL: e.SourceURL, Confidence: min(e.Confidence, pf.Confidence)})
			}
		}
		for _, num := range pf.Phones {
			if p, ok := phones[num]; ok {
				ownedPhones[num] = true
				b.edge(model.EdgeUpsert{Type: model.RelHasPhone, FromLabel: model.LabelPerson, FromKey: personID,
					ToLabel: model.LabelPhone, ToKey: num, SourceURL: p.SourceURL, Confidence: min(p.Confidence, pf.Confidence)})
			}
		}
		for _, s := range pf.Socials {
			b.node(model.NodeUpsert{Label: model.LabelSocial, Key: s.Value, Props: map[string]any{"network": s.Network, "handle": s.Handle}})
			b.edge(model.EdgeUpsert{Type: model.RelHasSocial, FromLabel: model.LabelPerson, FromKey: personID,
				ToLabel: model.LabelSocial, ToKey: s.Value, SourceURL: s.SourceURL, Confidence: s.Confidence})
		}
	}

	// Link published personal addresses to known people by name pattern.
	names := make(map[string]string, len(registry))
	for id, e := range registry {
		names[id] = e.Name
	}
	addrs := make([]string, 0, len(emails))
	for a := range emails {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		e := emails[addr]
		if !ownedEmails[addr] && e.Kind == model.EmailPersonal && sameSite(e.Domain, ex.Domain) {
			if personID, conf := MatchEmail(addr[:strings.IndexByte(addr, '@')], names); personID != "" {
				ownedEmails[addr] = true
				metrics.ResolveMerges.WithLabelValues("email_to_person").Inc()
				b.edge(model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelPerson, FromKey: personID,
					ToLabel: model.LabelEmail, ToKey: addr, SourceURL: e.SourceURL, Confidence: min(conf, e.Confidence)})
			}
		}
		if !ownedEmails[addr] {
			companyEdge(model.RelHasEmail, model.LabelEmail, addr, e.Fact)
		}
	}
	for _, p := range ex.Phones {
		if !ownedPhones[p.Value] {
			companyEdge(model.RelHasPhone, model.LabelPhone, p.Value, p.Fact)
		}
	}
	for _, s := range ex.Socials {
		b.node(model.NodeUpsert{Label: model.LabelSocial, Key: s.Value, Props: map[string]any{"network": s.Network, "handle": s.Handle}})
		companyEdge(model.RelHasSocial, model.LabelSocial, s.Value, s.Fact)
	}
	for _, t := range ex.Technologies {
		b.node(model.NodeUpsert{Label: model.LabelTechnology, Key: t.Value, Props: map[string]any{"category": t.Category}})
		companyEdge(model.RelUses, model.LabelTechnology, t.Value, t.Fact)
	}
	for _, a := range ex.Addresses {
		iso := normalize.Country(a.Country)
		if iso == "" {
			iso = a.Country
		}
		id := model.LocationID(iso, a.Region, a.City, a.Postal, normalize.Fold(a.Street))
		b.node(model.NodeUpsert{Label: model.LabelLocation, Key: id, Props: map[string]any{
			"country": iso, "region": a.Region, "city": a.City, "postal": a.Postal, "street": a.Street,
		}})
		companyEdge(model.RelLocatedAt, model.LabelLocation, id, a.Fact)
	}

	if err := r.links(ctx, companyID, ex, b); err != nil {
		return nil, err
	}
	return b.mut, nil
}

// companyCountry combines this page's evidence with what earlier pages of
// the same company established, keeping the strongest.
func (r *Resolver) companyCountry(ctx context.Context, companyID string, ex *model.Extraction) (string, int, error) {
	country, strength := CountryOf(ex)
	stored, err := r.rdb.Get(ctx, r.countryKey(companyID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", 0, err
	}
	if cc, s, ok := strings.Cut(stored, "|"); ok {
		if prev, _ := strconv.Atoi(s); prev >= strength {
			return cc, prev, nil
		}
	}
	if country != "" {
		if err := r.rdb.Set(ctx, r.countryKey(companyID), country+"|"+strconv.Itoa(strength), 0).Err(); err != nil {
			return "", 0, err
		}
	}
	return country, strength, nil
}

func (r *Resolver) loadPeople(ctx context.Context, companyID string) (map[string]personEntry, error) {
	raw, err := r.rdb.HGetAll(ctx, r.peopleKey(companyID)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]personEntry, len(raw))
	for id, v := range raw {
		var e personEntry
		if json.Unmarshal([]byte(v), &e) == nil {
			out[id] = e
		}
	}
	return out, nil
}

// resolvePerson returns the stable ID for a person, reusing the ID of an
// already-known person of the same company whose name matches. ok is false
// when the person is suppressed or has no usable name.
func (r *Resolver) resolvePerson(ctx context.Context, companyID string, pf model.PersonFact, registry map[string]personEntry) (string, bool, error) {
	nameKey := normalize.PersonKey(pf.Value)
	if nameKey == "" {
		return "", false, nil
	}
	linkedin := ""
	for _, s := range pf.Socials {
		if s.Network == "linkedin" {
			linkedin = s.Value
			break
		}
	}
	discriminator := linkedin
	if discriminator == "" {
		discriminator = normalize.TitleKey(pf.Title)
	}
	id := model.PersonID(companyID, nameKey, discriminator)

	if _, known := registry[id]; !known {
		// Fuzzy match against people already seen at this company. IDs are
		// visited in sorted order so the choice is deterministic.
		ids := make([]string, 0, len(registry))
		for k := range registry {
			ids = append(ids, k)
		}
		sort.Strings(ids)
		bestID, bestScore := "", 0.0
		for _, k := range ids {
			e := registry[k]
			if linkedin != "" && e.LinkedIn != "" && linkedin != e.LinkedIn {
				continue // two different profiles: two different people
			}
			if !normalize.SamePerson(nameKey, e.Name, r.cfg.Resolve.PersonSimilarity) {
				continue
			}
			if score := normalize.JaroWinkler(nameKey, e.Name); score > bestScore {
				bestID, bestScore = k, score
			}
		}
		if bestID != "" {
			id = bestID
			metrics.ResolveMerges.WithLabelValues("person").Inc()
		}
	}

	blocked, err := r.suppress.HasMembers(ctx, suppress.KindPerson, []string{id})
	if err != nil {
		return "", false, err
	}
	byName, err := r.suppress.HasMembers(ctx, suppress.KindPersonName, []string{model.PersonNameKey(companyID, nameKey)})
	if err != nil {
		return "", false, err
	}
	if blocked[0] || byName[0] {
		metrics.ResolveSuppressed.WithLabelValues("person").Inc()
		return "", false, nil
	}

	entry, known := registry[id]
	if !known {
		entry = personEntry{Name: nameKey}
	}
	if entry.LinkedIn == "" && linkedin != "" {
		entry.LinkedIn = linkedin
		known = false
	}
	if !known {
		raw, _ := json.Marshal(entry)
		if err := r.rdb.HSet(ctx, r.peopleKey(companyID), id, raw).Err(); err != nil {
			return "", false, err
		}
		registry[id] = entry
	}
	return id, true, nil
}

// links records outbound links and emits LINKS_TO edges. The writer only
// creates an edge when both companies exist, so a link to a company that is
// crawled later is remembered here and replayed when its homepage arrives.
func (r *Resolver) links(ctx context.Context, companyID string, ex *model.Extraction, b *builder) error {
	for _, d := range ex.OutboundDomains {
		if d == "" || d == ex.Domain {
			continue
		}
		key := r.linksKey(d)
		pipe := r.rdb.Pipeline()
		card := pipe.SCard(ctx, key)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		if card.Val() < maxInboundLinks {
			if err := r.rdb.SAdd(ctx, key, companyID+"\t"+ex.URL).Err(); err != nil {
				return err
			}
		}
		b.edge(model.EdgeUpsert{Type: model.RelLinksTo, FromLabel: model.LabelCompany, FromKey: companyID,
			ToLabel: model.LabelCompany, ToKey: model.CompanyID(d), Confidence: 0.9})
	}
	if ex.PageType != model.PageHome && ex.Depth != 0 {
		return nil
	}
	inbound, err := r.rdb.SMembers(ctx, r.linksKey(ex.Domain)).Result()
	if err != nil {
		return err
	}
	sort.Strings(inbound)
	for _, in := range inbound {
		from, source, ok := strings.Cut(in, "\t")
		if !ok || from == companyID {
			continue
		}
		b.edge(model.EdgeUpsert{Type: model.RelLinksTo, FromLabel: model.LabelCompany, FromKey: from,
			ToLabel: model.LabelCompany, ToKey: companyID, SourceURL: source, Confidence: 0.9})
	}
	return nil
}
