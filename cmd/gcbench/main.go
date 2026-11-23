// gcbench reproduces the article's core claim on your machine: a service with
// a small live heap but a large memory budget wastes CPU on GC under the
// default GOGC=100, and GOMEMLIMIT + GOGC=off removes most of that.
//
//	go run ./cmd/gcbench -live 32 -limit 512 -dur 5s
package main

import (
	"flag"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

var live [][]byte

func main() {
	liveMiB := flag.Int("live", 32, "live heap to hold (MiB)")
	limitMiB := flag.Int("limit", 512, "GOMEMLIMIT for the tuned run (MiB)")
	dur := flag.Duration("dur", 5*time.Second, "duration of each run")
	flag.Parse()

	live = make([][]byte, *liveMiB)
	for i := range live {
		live[i] = make([]byte, 1<<20)
	}

	fmt.Printf("live heap %d MiB, GOMAXPROCS %d, %s per run\n\n", *liveMiB, runtime.GOMAXPROCS(0), *dur)
	fmt.Printf("%-28s %10s %12s %12s %10s\n", "config", "GC cycles", "GC CPU (s)", "GC CPU %", "allocs/s")

	a := run("GOGC=100, no limit", 100, math.MaxInt64, *dur)
	b := run(fmt.Sprintf("GOGC=off, GOMEMLIMIT=%dMiB", *limitMiB), -1, int64(*limitMiB)<<20, *dur)

	if a.gcCPU > 0 {
		fmt.Printf("\nGC CPU reduced by %.1f%%, GC cycles %d -> %d\n",
			100*(1-b.gcCPU/a.gcCPU), a.cycles, b.cycles)
	}
}

type result struct {
	cycles uint64
	gcCPU  float64
}

func read() (cycles uint64, gcCPU, totalCPU float64) {
	s := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	metrics.Read(s)
	return s[0].Value.Uint64(), s[1].Value.Float64(), s[2].Value.Float64()
}

func run(name string, gcPercent int, limit int64, d time.Duration) result {
	debug.SetGCPercent(gcPercent)
	debug.SetMemoryLimit(limit)
	runtime.GC()

	c0, g0, t0 := read()
	var ops atomic.Uint64
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var keep []byte
			for time.Now().Before(deadline) {
				// Simulate request handling: lots of small short-lived allocs.
				for i := 0; i < 1000; i++ {
					keep = make([]byte, 512+i%2048)
				}
				_ = keep
				ops.Add(1000)
			}
		}()
	}
	wg.Wait()
	c1, g1, t1 := read()

	gc := g1 - g0
	pct := 0.0
	if t1-t0 > 0 {
		pct = 100 * gc / (t1 - t0)
	}
	fmt.Printf("%-28s %10d %12.3f %11.1f%% %10.2gM\n",
		name, c1-c0, gc, pct, float64(ops.Load())/d.Seconds()/1e6)
	return result{cycles: c1 - c0, gcCPU: gc}
}
