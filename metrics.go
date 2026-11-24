package goruntime

import (
	"bufio"
	"fmt"
	"math"
	"net/http"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
)

// MetricsHandler serves every metric in runtime/metrics (GC, heap, memory
// classes, CPU classes incl. GC CPU, scheduler) plus this library's view of
// the container/task limits, in Prometheus text format. No dependencies.
func (r *Runtime) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		bw := bufio.NewWriter(w)
		writeRuntimeMetrics(bw)
		r.writeLimitMetrics(bw)
		bw.Flush()
	})
}

var (
	descOnce sync.Once
	descs    []metrics.Description
	samples  []metrics.Sample
	sampleMu sync.Mutex
)

// promName converts "/gc/heap/allocs:bytes" -> "go_gc_heap_allocs_bytes".
func promName(runtimeName string) string {
	n := strings.TrimPrefix(runtimeName, "/")
	n = strings.NewReplacer("/", "_", ":", "_", "-", "_", "*", "x").Replace(n)
	return "go_" + n
}

func writeRuntimeMetrics(w *bufio.Writer) {
	descOnce.Do(func() {
		for _, d := range metrics.All() {
			if d.Kind == metrics.KindBad {
				continue
			}
			descs = append(descs, d)
			samples = append(samples, metrics.Sample{Name: d.Name})
		}
	})
	sampleMu.Lock()
	defer sampleMu.Unlock()
	metrics.Read(samples)

	for i, s := range samples {
		d := descs[i]
		name := promName(d.Name)
		help := strings.ReplaceAll(d.Description, "\n", " ")
		switch s.Value.Kind() {
		case metrics.KindUint64:
			writeHeader(w, name, help, counterOrGauge(d))
			v := s.Value.Uint64()
			// /gc/gogc:percent is a uint64, so GOGC=off (-1) wraps to MaxUint64.
			if d.Name == "/gc/gogc:percent" && v == math.MaxUint64 {
				fmt.Fprintf(w, "%s -1\n", name)
			} else {
				fmt.Fprintf(w, "%s %d\n", name, v)
			}
		case metrics.KindFloat64:
			writeHeader(w, name, help, counterOrGauge(d))
			fmt.Fprintf(w, "%s %g\n", name, s.Value.Float64())
		case metrics.KindFloat64Histogram:
			writeHistogram(w, name, help, s.Value.Float64Histogram())
		}
	}
}

func counterOrGauge(d metrics.Description) string {
	if d.Cumulative {
		return "counter"
	}
	return "gauge"
}

func writeHeader(w *bufio.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

// Runtime histograms have many fine buckets; collapse to at most ~16 so the
// scrape stays small. Buckets[i] is the lower bound of Counts[i].
func writeHistogram(w *bufio.Writer, name, help string, h *metrics.Float64Histogram) {
	writeHeader(w, name, help, "histogram")
	const maxBuckets = 16
	step := (len(h.Counts) + maxBuckets - 1) / maxBuckets
	if step < 1 {
		step = 1
	}
	var cum uint64
	var sum float64
	for i, c := range h.Counts {
		cum += c
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		mid := lo
		if !math.IsInf(lo, 0) && !math.IsInf(hi, 0) {
			mid = (lo + hi) / 2
		} else if math.IsInf(lo, -1) {
			mid = hi
		}
		if !math.IsInf(mid, 0) {
			sum += mid * float64(c)
		}
		last := i == len(h.Counts)-1
		if (i+1)%step == 0 || last {
			if math.IsInf(hi, 1) {
				continue // emitted as +Inf below
			}
			fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, hi, cum)
		}
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cum)
	fmt.Fprintf(w, "%s_sum %g\n", name, sum)
	fmt.Fprintf(w, "%s_count %d\n", name, cum)
}

func (r *Runtime) writeLimitMetrics(w *bufio.Writer) {
	st := r.State()
	g := func(name, help string, v float64) {
		writeHeader(w, name, help, "gauge")
		fmt.Fprintf(w, "%s %s\n", name, strconv.FormatFloat(v, 'f', -1, 64))
	}
	c := func(name, help string, v uint64) {
		writeHeader(w, name, help, "counter")
		fmt.Fprintf(w, "%s %d\n", name, v)
	}
	g("goruntime_gomemlimit_bytes", "GOMEMLIMIT currently applied by goruntime (0 = none).", float64(st.MemoryLimit))
	g("goruntime_gomaxprocs", "GOMAXPROCS in effect.", float64(st.GOMAXPROCS))
	g("goruntime_cpu_limit_cores", "CPU hard limit GOMAXPROCS was derived from.", st.CPULimit)
	g("goruntime_task_memory_limit_bytes", "ECS task (or cgroup) memory hard limit.", float64(st.Snapshot.TaskMemoryLimit))
	g("goruntime_task_memory_usage_bytes", "Working set of all containers in the task.", float64(st.Snapshot.TaskMemoryUsage))
	g("goruntime_container_memory_limit_bytes", "This container's memory hard limit (0 = unset).", float64(st.Snapshot.ContainerMemoryLimit))
	g("goruntime_container_memory_usage_bytes", "This container's working set.", float64(st.Snapshot.ContainerMemoryUsage))
	g("goruntime_others_memory_peak_bytes", "Peak working set of the other containers in the task over the peak window; GOMEMLIMIT is sized from this.", float64(st.OthersMemoryPeak))
	if st.Snapshot.TaskMemoryLimit > 0 {
		g("goruntime_task_memory_utilization_ratio", "Task memory usage / limit.",
			float64(st.Snapshot.TaskMemoryUsage)/float64(st.Snapshot.TaskMemoryLimit))
	}
	c("goruntime_gomemlimit_updates_total", "Times GOMEMLIMIT was changed.", st.LimitUpdates)
	c("goruntime_refresh_errors_total", "Failed attempts to read limits/usage.", st.RefreshErrors)
}
