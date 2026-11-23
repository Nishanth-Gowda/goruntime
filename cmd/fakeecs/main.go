// fakeecs serves a fake ECS Task Metadata Endpoint v4 so the library can be
// exercised locally. The task has two containers: "app" (us) and "envoy",
// a sidecar whose memory usage swings up and down over time.
//
//	go run ./cmd/fakeecs -task-mem 1024 -task-cpu 2
//	ECS_CONTAINER_METADATA_URI_V4=http://localhost:9100/v4/app go run ./cmd/demo
package main

import (
	"encoding/json"
	"flag"
	"log"
	"math"
	"net/http"
	"time"
)

func main() {
	addr := flag.String("addr", ":9100", "listen address")
	taskMem := flag.Int("task-mem", 1024, "task memory limit (MiB)")
	taskCPU := flag.Float64("task-cpu", 2, "task CPU limit (vCPU)")
	appMem := flag.Int("app-mem", 0, "app container memory limit (MiB, 0 = unset)")
	appUsage := flag.Int("app-usage", 200, "reported app working set (MiB)")
	sidecarMin := flag.Int("sidecar-min", 50, "sidecar min usage (MiB)")
	sidecarMax := flag.Int("sidecar-max", 400, "sidecar max usage (MiB)")
	period := flag.Duration("period", time.Minute, "sidecar usage oscillation period")
	flag.Parse()

	start := time.Now()
	sidecar := func() uint64 {
		phase := float64(time.Since(start)) / float64(*period) * 2 * math.Pi
		f := (math.Sin(phase) + 1) / 2
		return uint64(float64(*sidecarMin)+f*float64(*sidecarMax-*sidecarMin)) << 20
	}
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}
	container := func(id string, memMiB int) map[string]any {
		return map[string]any{
			"DockerId": id, "Name": id,
			"Limits": map[string]any{"CPU": 0, "Memory": memMiB},
		}
	}
	stats := func(usage uint64) map[string]any {
		return map[string]any{"memory_stats": map[string]any{
			"usage": usage + 20<<20, "limit": uint64(math.MaxInt64),
			"stats": map[string]uint64{"inactive_file": 20 << 20},
		}}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v4/app", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, container("app", *appMem))
	})
	mux.HandleFunc("GET /v4/app/task", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{
			"Limits":     map[string]any{"CPU": *taskCPU, "Memory": *taskMem},
			"Containers": []any{container("app", *appMem), container("envoy", 0)},
		})
	})
	mux.HandleFunc("GET /v4/app/task/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := sidecar()
		log.Printf("stats: envoy=%dMiB app=%dMiB", s>>20, *appUsage)
		writeJSON(w, map[string]any{
			"app":   stats(uint64(*appUsage) << 20),
			"envoy": stats(s),
		})
	})
	log.Printf("fake ECS metadata on %s (task %d MiB / %.2f vCPU)", *addr, *taskMem, *taskCPU)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
