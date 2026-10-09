package fetch

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/chromedp"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/extract"
)

// Renderer loads a page in a real browser and returns the rendered DOM.
type Renderer interface {
	Render(ctx context.Context, url string) ([]byte, error)
	Close()
}

// NeedsRender reports whether a statically fetched HTML document is an empty
// JavaScript shell: almost no visible text but at least one script.
func NeedsRender(body []byte, minTextBytes int) bool {
	text, scripts := extract.TextDensity(body)
	return scripts > 0 && text < minTextBytes
}

// ChromeRenderer renders with headless Chrome via chromedp. It has its own
// small worker pool, separate from the HTTP fetch workers, because a browser
// tab costs orders of magnitude more than a GET.
type ChromeRenderer struct {
	cfg       config.Headless
	userAgent string
	// hostRules mirrors fetch.host_overrides for the browser's resolver.
	hostRules string
	sem       chan struct{}

	mu          sync.Mutex
	allocCtx    context.Context
	allocCancel context.CancelFunc
	browserCtx  context.Context
	browserStop context.CancelFunc
}

// NewChromeRenderer prepares a renderer; Chrome starts on first use.
// hostOverrides (host -> ip:port) is applied to the browser's resolver so it
// reaches the same servers as the HTTP client.
func NewChromeRenderer(cfg config.Headless, userAgent string, hostOverrides map[string]string) *ChromeRenderer {
	hosts := make([]string, 0, len(hostOverrides))
	for h := range hostOverrides {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	rules := make([]string, 0, len(hosts))
	for _, h := range hosts {
		rules = append(rules, "MAP "+h+" "+hostOverrides[h])
	}
	return &ChromeRenderer{cfg: cfg, userAgent: userAgent, hostRules: strings.Join(rules, ", "),
		sem: make(chan struct{}, max(cfg.Workers, 1))}
}

func (r *ChromeRenderer) browser() (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.browserCtx != nil && r.browserCtx.Err() == nil {
		return r.browserCtx, nil
	}
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserAgent(r.userAgent),
		chromedp.Flag("blink-settings", "imagesEnabled=false"),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("mute-audio", true),
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
	)
	if r.hostRules != "" {
		opts = append(opts, chromedp.Flag("host-resolver-rules", r.hostRules))
	}
	if r.cfg.ExecPath != "" {
		opts = append(opts, chromedp.ExecPath(r.cfg.ExecPath))
	}
	r.allocCtx, r.allocCancel = chromedp.NewExecAllocator(context.Background(), opts...)
	r.browserCtx, r.browserStop = chromedp.NewContext(r.allocCtx)
	if _, err := chromedp.Run(r.browserCtx, chromedp.Navigate("about:blank")); err != nil {
		r.browserStop()
		r.allocCancel()
		r.browserCtx = nil
		return nil, err
	}
	return r.browserCtx, nil
}

// Render navigates to url, waits for the document to settle and returns the
// serialized DOM. It only reads: no input, no form submission, no login.
func (r *ChromeRenderer) Render(ctx context.Context, url string) ([]byte, error) {
	select {
	case r.sem <- struct{}{}:
		defer func() { <-r.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	browser, err := r.browser()
	if err != nil {
		return nil, err
	}
	tab, cancelTab := chromedp.NewContext(browser)
	defer cancelTab()
	tab, cancelTimeout := context.WithTimeout(tab, r.cfg.Timeout)
	defer cancelTimeout()
	stop := context.AfterFunc(ctx, cancelTab)
	defer stop()

	err = chromedp.Do(tab,
		chromedp.Navigate(url),
		chromedp.WaitReady("body"),
		chromedp.Sleep(750*time.Millisecond),
	)
	var html string
	if err == nil {
		html, err = chromedp.Run(tab, chromedp.OuterHTML("html"))
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if html == "" {
		return nil, errors.New("fetch: headless render produced no document")
	}
	return []byte(html), nil
}

// Close shuts the browser down.
func (r *ChromeRenderer) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.browserStop != nil {
		r.browserStop()
		r.allocCancel()
		r.browserCtx = nil
	}
}
