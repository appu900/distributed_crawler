// Command fixturesite serves the built-in fixture websites (acme.test and
// globex.test) on one local port, routed by Host header. The demo points the
// crawler at it with fetch.host_overrides.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/appu/leadgraph/internal/testsite"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8089", "listen address")
	flag.Parse()

	srv := &http.Server{Addr: *addr, Handler: testsite.New(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	fmt.Fprintf(os.Stderr, "fixture sites %s and %s listening on %s\n", testsite.Acme, testsite.Globex, *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
