// demo is a small HTTP service wired up with goruntime.
//
//	GET /metrics          Prometheus metrics (runtime + limits)
//	GET /state            JSON of what goruntime applied
//	GET /work?mb=50       allocate ~50 MiB of short-lived garbage
//	GET /debug/pprof/...  profiling
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Nishanth-Gowda/goruntime"
)

var sink atomic.Pointer[[]byte] // atomic: handlers run concurrently

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	interval := flag.Duration("interval", 10*time.Second, "limit refresh interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, _ := goruntime.Start(ctx, goruntime.Config{Interval: *interval})
	defer rt.Stop()

	http.Handle("/metrics", rt.MetricsHandler())
	http.HandleFunc("/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(rt.State())
	})
	http.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		mb, _ := strconv.Atoi(r.URL.Query().Get("mb"))
		if mb <= 0 {
			mb = 10
		}
		buf := make([][]byte, 0, mb*16)
		for i := 0; i < mb*16; i++ {
			buf = append(buf, make([]byte, 64<<10))
		}
		// Keep one 64 KiB buffer so the work isn't optimized away. Storing
		// buf[:1] would keep the whole backing array (and every buffer) alive.
		keep := buf[0]
		sink.Store(&keep)
		w.Write([]byte("ok\n"))
	})

	srv := &http.Server{Addr: *addr, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	slog.Info("listening", "addr", *addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
