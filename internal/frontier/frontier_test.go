package frontier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/pkg/model"
)

type fixture struct {
	*Frontier
	rdb     *redis.Client
	streams *queue.Streams
	clock   time.Time
}

func newFixture(t *testing.T, mutate func(*config.Crawl)) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	cfg := config.Default().Crawl
	cfg.GlobalRPS = 1000
	if mutate != nil {
		mutate(&cfg)
	}
	streams := queue.NewStreams(rdb, "t", 0)
	fx := &fixture{rdb: rdb, streams: streams, clock: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	fx.Frontier = New(rdb, streams, "t", cfg, nil)
	fx.Frontier.now = func() time.Time { return fx.clock }
	return fx
}

func page(url string, depth int) model.FetchRequest {
	return model.FetchRequest{URL: url, Kind: model.KindPage, Depth: depth}
}

// drain returns the requests currently in the fetch stream and removes them.
func (fx *fixture) drain(t *testing.T) []model.FetchRequest {
	t.Helper()
	ctx := context.Background()
	entries, err := fx.rdb.XRange(ctx, fx.streams.Key(queue.Fetch), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	var out []model.FetchRequest
	for _, e := range entries {
		r, err := Decode(e.Values["d"].(string))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
		fx.rdb.XDel(ctx, fx.streams.Key(queue.Fetch), e.ID)
	}
	return out
}

func (fx *fixture) dispatch(t *testing.T) []model.FetchRequest {
	t.Helper()
	if _, err := fx.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	return fx.drain(t)
}

func TestEncodeDecode(t *testing.T) {
	in := model.FetchRequest{URL: "https://acme.com/a?b=1\tc", Kind: model.KindSitemap, Depth: 2, Attempt: 1, Hops: 3, Probe: true, Domain: "acme.com"}
	out, err := Decode(encode(in))
	if err != nil || out != in {
		t.Errorf("round trip: %+v, %v", out, err)
	}
	in.Seed, in.Probe = true, false
	if out, _ := Decode(encode(in)); out != in {
		t.Errorf("seed flag: %+v", out)
	}
	for _, bad := range []string{"", "p\t1", "x\t0\t0\t-\t0\td\tu", "p\tz\t0\t-\t0\td\tu", "p\t0\t0\t-\td\tu"} {
		if _, err := Decode(bad); err == nil {
			t.Errorf("Decode(%q) should fail", bad)
		}
	}
}

func TestPriorityOrder(t *testing.T) {
	home := Priority(model.FetchRequest{URL: "https://a.com/", Seed: true})
	about := Priority(page("https://a.com/about", 1))
	team := Priority(page("https://a.com/team", 1))
	deepTeam := Priority(page("https://a.com/x/team", 3))
	blog := Priority(page("https://a.com/blog/post", 1))
	sitemap := Priority(model.FetchRequest{URL: "https://a.com/sitemap.xml", Kind: model.KindSitemap, Depth: 1})
	if !(home < about && about == team && team < deepTeam && deepTeam < blog && sitemap < about) {
		t.Errorf("priorities: home=%v about=%v team=%v deepTeam=%v blog=%v sitemap=%v", home, about, team, deepTeam, blog, sitemap)
	}
}

func TestAddDedupAndLimits(t *testing.T) {
	fx := newFixture(t, func(c *config.Crawl) { c.MaxQueuePerHost = 3; c.MaxDepth = 2 })
	ctx := context.Background()
	check := func(r model.FetchRequest, force bool, want Outcome) {
		t.Helper()
		got, err := fx.Add(ctx, r, force)
		if err != nil || got != want {
			t.Errorf("Add(%s) = %v, %v; want %v", r.URL, got, err, want)
		}
	}
	check(page("https://acme.com/a", 1), false, Added)
	check(page("https://acme.com/a", 1), false, Duplicate)
	check(page("https://acme.com/a", 1), true, Added) // same member: still one entry
	check(page("https://acme.com/deep", 3), false, Rejected)
	check(page("::bad", 0), false, Rejected)
	check(page("https://acme.com/b", 1), false, Added)
	check(page("https://acme.com/c", 1), false, Added)
	check(page("https://acme.com/d", 1), false, QueueFull)
	// A URL dropped for a full queue is not marked seen: it can come back.
	fx.dispatch(t)
	check(page("https://acme.com/d", 1), false, Added)
	// Other hosts have their own queue.
	check(page("https://other.com/a", 1), false, Added)

	fx.Blocked = func(_ context.Context, d string) bool { return d == "blocked.com" }
	check(page("https://www.blocked.com/", 0), false, Rejected)
	if _, err := fx.Seed(ctx, "blocked.com"); err == nil {
		t.Error("seeding a blocked domain should fail")
	}
}

func TestSeedExpansion(t *testing.T) {
	fx := newFixture(t, nil)
	ctx := context.Background()
	n, err := fx.Seed(ctx, "Acme.com")
	if err != nil || n != 9 {
		t.Fatalf("seeded %d, err %v; want homepage + 8 well-known paths", n, err)
	}
	if n, _ := fx.Seed(ctx, "https://acme.com/"); n != 0 {
		t.Errorf("re-seeding added %d", n)
	}
	if _, err := fx.Seed(ctx, "not a domain"); err == nil {
		t.Error("bad seed should fail")
	}
	// The homepage goes first, then the well-known pages, one per round
	// because the host is leased between fetches.
	first := fx.dispatch(t)
	if len(first) != 1 || first[0].URL != "https://acme.com/" || !first[0].Seed || first[0].Domain != "acme.com" {
		t.Fatalf("first dispatch = %+v", first)
	}
	if again := fx.dispatch(t); len(again) != 0 {
		t.Fatalf("host dispatched twice while leased: %+v", again)
	}
	fx.Release(ctx, "acme.com", fx.clock)
	second := fx.dispatch(t)
	if len(second) != 1 || !second[0].Probe || second[0].Depth != 1 {
		t.Errorf("second dispatch = %+v", second)
	}
}

func TestPolitenessDelayAndLease(t *testing.T) {
	fx := newFixture(t, func(c *config.Crawl) { c.HostLease = time.Minute })
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		fx.Add(ctx, page(fmt.Sprintf("https://acme.com/p%d", i), 1), false)
		fx.Add(ctx, page(fmt.Sprintf("https://globex.com/p%d", i), 1), false)
	}
	got := fx.dispatch(t)
	if len(got) != 2 || got[0].Domain == got[1].Domain {
		t.Fatalf("want one URL per host, got %+v", got)
	}
	// acme finishes and must wait 5s (e.g. Crawl-delay); globex never reports back.
	fx.Release(ctx, "acme.com", fx.clock.Add(5*time.Second))
	fx.clock = fx.clock.Add(4 * time.Second)
	if got := fx.dispatch(t); len(got) != 0 {
		t.Fatalf("dispatched before the delay elapsed: %+v", got)
	}
	fx.clock = fx.clock.Add(time.Second)
	if got := fx.dispatch(t); len(got) != 1 || got[0].Domain != "acme.com" {
		t.Fatalf("acme should be due after its delay: %+v", got)
	}
	// A crashed fetcher's lease expires, so the host is not stuck forever.
	fx.clock = fx.clock.Add(time.Minute)
	got = fx.dispatch(t)
	hosts := map[string]bool{}
	for _, r := range got {
		hosts[r.Domain] = true
	}
	if !hosts["globex.com"] {
		t.Errorf("lease did not expire: %+v", got)
	}
	// Release of an unknown host is a no-op, not a phantom entry.
	fx.Release(ctx, "nobody.com", fx.clock)
	if n, _ := fx.rdb.ZScore(ctx, "t:hosts", "nobody.com").Result(); n != 0 {
		t.Error("release created a host entry")
	}
}

func TestPageBudgetAndPriority(t *testing.T) {
	fx := newFixture(t, func(c *config.Crawl) { c.MaxPagesPerDomain = 4 })
	ctx := context.Background()
	// Discovery order is worst-first; priority must reorder it.
	for i := 0; i < 10; i++ {
		fx.Add(ctx, page(fmt.Sprintf("https://acme.com/blog/%d", i), 2), false)
	}
	fx.Add(ctx, page("https://acme.com/contact", 2), false)
	fx.Add(ctx, page("https://acme.com/team", 1), false)
	fx.Add(ctx, model.FetchRequest{URL: "https://acme.com/", Kind: model.KindPage, Seed: true}, false)
	fx.Add(ctx, model.FetchRequest{URL: "https://acme.com/sitemap.xml", Kind: model.KindSitemap, Depth: 1}, false)

	var order []string
	for i := 0; i < 20; i++ {
		got := fx.dispatch(t)
		for _, r := range got {
			order = append(order, r.URL)
		}
		fx.Release(ctx, "acme.com", fx.clock)
	}
	want := []string{"https://acme.com/", "https://acme.com/sitemap.xml", "https://acme.com/team", "https://acme.com/contact"}
	if len(order) != 5 {
		t.Fatalf("dispatched %d URLs (%v); budget is 4 pages plus the sitemap", len(order), order)
	}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("order[%d] = %s, want %s", i, order[i], w)
		}
	}
	if idle, _ := fx.Idle(ctx); !idle {
		t.Error("an exhausted domain should leave the frontier idle")
	}
	// A subdomain shares the registrable domain's budget.
	fx.Add(ctx, page("https://blog.acme.com/x", 1), false)
	if got := fx.dispatch(t); len(got) != 0 {
		t.Errorf("subdomain exceeded the domain budget: %+v", got)
	}
}

func TestRequeueKeepsBudget(t *testing.T) {
	fx := newFixture(t, func(c *config.Crawl) { c.MaxPagesPerDomain = 1 })
	ctx := context.Background()
	fx.Add(ctx, page("https://acme.com/a", 1), false)
	got := fx.dispatch(t)
	if len(got) != 1 {
		t.Fatal("expected one dispatch")
	}
	retryAt := fx.clock.Add(30 * time.Second)
	if err := fx.Requeue(ctx, got[0], retryAt); err != nil {
		t.Fatal(err)
	}
	if got := fx.dispatch(t); len(got) != 0 {
		t.Fatal("requeued URL dispatched before its hold time")
	}
	fx.clock = retryAt
	again := fx.dispatch(t)
	if len(again) != 1 || again[0].Attempt != 1 || again[0].URL != "https://acme.com/a" {
		t.Errorf("requeue = %+v; a retry must not be charged to the page budget", again)
	}
}

func TestGlobalRateLimit(t *testing.T) {
	fx := newFixture(t, func(c *config.Crawl) { c.GlobalRPS = 5; c.DispatchBatch = 100 })
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		fx.Add(ctx, page(fmt.Sprintf("https://site%d.com/", i), 0), false)
	}
	total := 0
	for i := 0; i < 3; i++ {
		total += len(fx.dispatch(t))
	}
	if total != 5 {
		t.Errorf("dispatched %d in one second, limit is 5", total)
	}
	fx.clock = fx.clock.Add(time.Second)
	if n := len(fx.dispatch(t)); n != 5 {
		t.Errorf("next second dispatched %d, want 5", n)
	}
}

func TestKillSwitch(t *testing.T) {
	fx := newFixture(t, nil)
	ctx := context.Background()
	fx.Add(ctx, page("https://acme.com/", 0), false)
	if err := fx.SetKill(ctx, true, "abuse report"); err != nil {
		t.Fatal(err)
	}
	on, reason, err := fx.Killed(ctx)
	if err != nil || !on || reason == "" {
		t.Fatalf("killed=%v reason=%q err=%v", on, reason, err)
	}
	if got := fx.dispatch(t); len(got) != 0 {
		t.Fatalf("dispatched while killed: %+v", got)
	}
	fx.SetKill(ctx, false, "")
	if on, _, _ := fx.Killed(ctx); on {
		t.Fatal("kill switch did not clear")
	}
	if got := fx.dispatch(t); len(got) != 1 {
		t.Errorf("dispatch after clearing = %+v", got)
	}
}

func TestIdleAndStats(t *testing.T) {
	fx := newFixture(t, nil)
	ctx := context.Background()
	if idle, _ := fx.Idle(ctx); !idle {
		t.Error("empty frontier should be idle")
	}
	fx.Add(ctx, page("https://acme.com/a", 1), false)
	fx.Add(ctx, page("https://acme.com/b", 1), false)
	st, _ := fx.Stats(ctx)
	if st.Hosts != 1 || st.Queued != 2 {
		t.Errorf("stats = %+v", st)
	}
	fx.dispatch(t)
	fx.Release(ctx, "acme.com", fx.clock)
	fx.dispatch(t)
	if idle, _ := fx.Idle(ctx); idle {
		t.Error("a leased host means work is in flight")
	}
	fx.Release(ctx, "acme.com", fx.clock)
	fx.dispatch(t) // queue empty and host due: host is retired
	if idle, _ := fx.Idle(ctx); !idle {
		t.Error("frontier should be idle once the last host is released and drained")
	}
	fx.Add(ctx, page("https://acme.com/c", 1), false)
	fx.DropHost(ctx, "acme.com")
	if idle, _ := fx.Idle(ctx); !idle {
		t.Error("DropHost should clear the host")
	}
}
