package goruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeMemLimit(t *testing.T) {
	const M = mib
	cases := []struct {
		name string
		s    Snapshot
		want uint64
	}{
		{"no limits", Snapshot{}, 0},
		{"task only, no sidecars", Snapshot{TaskMemoryLimit: 1000 * M}, 900 * M},
		{"container limit tighter than task", Snapshot{TaskMemoryLimit: 1000 * M, ContainerMemoryLimit: 500 * M}, 450 * M},
		{"sidecar uses 300", Snapshot{
			TaskMemoryLimit: 1000 * M, TaskMemoryUsage: 500 * M, ContainerMemoryUsage: 200 * M,
		}, 630 * M}, // (1000-300)*0.9
		{"sidecar hogs memory -> floor", Snapshot{
			TaskMemoryLimit: 1000 * M, TaskMemoryUsage: 950 * M, ContainerMemoryUsage: 100 * M,
		}, 500 * M}, // floor = 0.5 * 1000
		{"sidecar over task limit -> floor", Snapshot{
			TaskMemoryLimit: 1000 * M, TaskMemoryUsage: 2000 * M,
		}, 500 * M},
	}
	for _, c := range cases {
		if got := ComputeMemLimit(c.s, MemLimitPolicy{}); got != c.want {
			t.Errorf("%s: got %d MiB, want %d MiB", c.name, got/M, c.want/M)
		}
	}
}

func TestComputeMaxProcs(t *testing.T) {
	cases := []struct {
		cpu  float64
		n    int
		want int
	}{{0, 8, 0}, {0.25, 8, 1}, {1.5, 8, 1}, {2, 8, 2}, {3.9, 8, 3}, {16, 8, 8}}
	for _, c := range cases {
		if got := ComputeMaxProcs(c.cpu, c.n); got != c.want {
			t.Errorf("ComputeMaxProcs(%v,%d)=%d want %d", c.cpu, c.n, got, c.want)
		}
	}
}

func fakeECS(t *testing.T, sidecarMiB *atomic.Uint64) *httptest.Server {
	t.Helper()
	j := func(w http.ResponseWriter, v any) { json.NewEncoder(w).Encode(v) }
	mux := http.NewServeMux()
	mux.HandleFunc("/v4/me", func(w http.ResponseWriter, _ *http.Request) {
		j(w, map[string]any{"DockerId": "me"})
	})
	mux.HandleFunc("/v4/me/task", func(w http.ResponseWriter, _ *http.Request) {
		j(w, map[string]any{
			"Limits": map[string]any{"CPU": 2.0, "Memory": 1000},
			"Containers": []any{
				map[string]any{"DockerId": "me", "Limits": map[string]any{"CPU": 1024, "Memory": 0}},
				map[string]any{"DockerId": "envoy", "Limits": map[string]any{"CPU": 0, "Memory": 0}},
			},
		})
	})
	mux.HandleFunc("/v4/me/task/stats", func(w http.ResponseWriter, _ *http.Request) {
		st := func(u uint64) any {
			return map[string]any{"memory_stats": map[string]any{
				"usage": u + 10*mib, "limit": uint64(1 << 62),
				"stats": map[string]uint64{"inactive_file": 10 * mib},
			}}
		}
		j(w, map[string]any{"me": st(200 * mib), "envoy": st(sidecarMiB.Load() * mib)})
	})
	return httptest.NewServer(mux)
}

func TestECSSnapshot(t *testing.T) {
	var side atomic.Uint64
	side.Store(300)
	srv := fakeECS(t, &side)
	defer srv.Close()
	c := &ecsClient{base: srv.URL + "/v4/me", hc: srv.Client()}
	s, err := c.snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.TaskMemoryLimit != 1000*mib || s.TaskCPULimit != 2 || s.ContainerCPULimit != 1 {
		t.Fatalf("limits: %+v", s)
	}
	if s.ContainerMemoryUsage != 200*mib || s.TaskMemoryUsage != 500*mib {
		t.Fatalf("usage: %+v", s)
	}
	if s.ContainerMemoryLimit != 0 {
		t.Fatalf("container limit should stay unset: %d", s.ContainerMemoryLimit)
	}
}

func TestRuntimeTracksSidecar(t *testing.T) {
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(-1))
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	var side atomic.Uint64
	side.Store(100)
	srv := fakeECS(t, &side)
	defer srv.Close()
	c := &ecsClient{base: srv.URL + "/v4/me", hc: srv.Client()}

	rt, err := Start(context.Background(), Config{
		Interval:        20 * time.Millisecond,
		DisableMaxProcs: true,
		snapshotFn:      c.snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	if got := rt.State().MemoryLimit; got != 810*mib { // (1000-100)*0.9
		t.Fatalf("initial limit %d MiB", got/mib)
	}
	if got := debug.SetMemoryLimit(-1); got != 810*mib {
		t.Fatalf("runtime limit %d", got)
	}

	side.Store(400) // sidecar grows; we must shrink
	deadline := time.Now().Add(2 * time.Second)
	for rt.State().MemoryLimit != 540*mib { // (1000-400)*0.9
		if time.Now().After(deadline) {
			t.Fatalf("limit did not follow sidecar: %d MiB", rt.State().MemoryLimit/mib)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCgroupV2(t *testing.T) {
	dir := t.TempDir()
	w := func(name, v string) { os.WriteFile(filepath.Join(dir, name), []byte(v), 0o644) }
	w("memory.max", "536870912\n")
	w("memory.current", "104857600\n")
	w("memory.stat", "anon 1\ninactive_file 4857600\n")
	w("cpu.max", "150000 100000\n")
	old := cgroupRoot
	cgroupRoot = dir
	defer func() { cgroupRoot = old }()

	s, ok := cgroupSnapshot()
	if !ok || s.ContainerMemoryLimit != 512*mib || s.ContainerCPULimit != 1.5 || s.ContainerMemoryUsage != 100000000 {
		t.Fatalf("%v %+v", ok, s)
	}
	if ComputeMaxProcs(s.CPULimit(), 8) != 1 {
		t.Fatal("maxprocs")
	}
}

func TestMetricsHandler(t *testing.T) {
	rt, _ := Start(context.Background(), Config{
		DisableMaxProcs: true, DisableMemLimit: true,
		snapshotFn: func(context.Context) (Snapshot, error) { return Snapshot{}, nil },
	})
	defer rt.Stop()
	rec := httptest.NewRecorder()
	rt.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	rec2 := httptest.NewRecorder()
	rt.MetricsHandler().ServeHTTP(rec2, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec2.Body.String(), "\ngo_gc_gogc_percent -1\n") {
		t.Error("GOGC=off should be exported as go_gc_gogc_percent -1")
	}

	for _, want := range []string{
		"go_gc_cycles_total_gc_cycles ",
		"go_cpu_classes_gc_total_cpu_seconds ",
		"go_gc_heap_live_bytes ",
		"go_gc_pauses_seconds_bucket{le=\"+Inf\"}",
		"goruntime_gomemlimit_bytes ",
		"goruntime_gomaxprocs ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	// Every non-comment line must be "name[{labels}] value".
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		l := sc.Text()
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		if len(strings.Fields(l)) != 2 {
			t.Fatalf("bad line %q", l)
		}
	}
}

func TestPeakWindow(t *testing.T) {
	t0 := time.Unix(0, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	p := peakWindow{window: time.Minute}
	steps := []struct {
		sec  int
		v    uint64
		want uint64
	}{
		{0, 100, 100},
		{10, 300, 300}, // grows: new peak at once
		{20, 50, 300},  // shrinks: peak held
		{69, 40, 300},  // 300@10s still inside (9s, 69s]
		{75, 40, 50},   // 300 expired; 50@20s is the peak now
		{81, 40, 40},   // 50 expired too
		{90, 500, 500},
	}
	for _, s := range steps {
		if got := p.add(at(s.sec), s.v); got != s.want {
			t.Fatalf("t=%ds add(%d)=%d want %d", s.sec, s.v, got, s.want)
		}
	}
	if got := (&peakWindow{}).add(t0, 7); got != 7 {
		t.Fatalf("no window should return the latest value, got %d", got)
	}
}

func TestRuntimeShrinksFastGrowsSlow(t *testing.T) {
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(-1))
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	now := time.Unix(0, 0)
	snap := func(othersMiB uint64) Snapshot {
		return Snapshot{
			Source: "ecs", TaskMemoryLimit: 1000 * mib,
			ContainerMemoryUsage: 200 * mib, TaskMemoryUsage: (200 + othersMiB) * mib,
		}
	}
	rt, err := Start(context.Background(), Config{
		Interval:        time.Hour, // the loop never fires; the test drives apply
		FastInterval:    time.Minute,
		PeakWindow:      2 * time.Minute,
		DisableMaxProcs: true,
		snapshotFn:      func(context.Context) (Snapshot, error) { return snap(100), nil },
		now:             func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	step := func(d time.Duration, othersMiB, wantMiB uint64, wantNext time.Duration) {
		t.Helper()
		now = now.Add(d)
		next := rt.apply(snap(othersMiB), false)
		if got := rt.State().MemoryLimit; got != wantMiB*mib {
			t.Fatalf("others=%d MiB: limit %d MiB, want %d", othersMiB, got/mib, wantMiB)
		}
		if next != wantNext {
			t.Fatalf("others=%d MiB: next refresh in %v, want %v", othersMiB, next, wantNext)
		}
	}
	if got := rt.State().MemoryLimit; got != 810*mib { // (1000-100)*0.9
		t.Fatalf("initial limit %d MiB", got/mib)
	}
	step(10*time.Second, 400, 540, time.Minute) // sidecar grows: shrink now, poll fast
	step(10*time.Second, 400, 540, time.Hour)   // steady: back to normal polling
	step(10*time.Second, 100, 540, time.Hour)   // sidecar shrinks: hold the limit
	step(90*time.Second, 100, 540, time.Hour)   // last 400 reading (t=20s) is 100s old: still held
	step(25*time.Second, 100, 810, time.Hour)   // 125s old, past the 2m window: give memory back
	step(10*time.Second, 105, 810, time.Hour)   // +5 MiB < 1% of task: no fast poll, change skipped
	if got := rt.State().OthersMemoryPeak; got != 105*mib {
		t.Fatalf("OthersMemoryPeak %d MiB", got/mib)
	}
}
