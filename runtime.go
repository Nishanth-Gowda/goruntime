// Package goruntime makes a Go service container-aware:
//
//   - sets GOMAXPROCS from the ECS task (or cgroup) CPU hard limit,
//   - keeps GOMEMLIMIT tuned at runtime from the ECS task / container memory
//     limits and live usage of every container in the task,
//   - exports Go runtime + limit metrics in Prometheus format.
//
// Usage:
//
//	rt, _ := goruntime.Start(ctx, goruntime.Config{})
//	http.Handle("/metrics", rt.MetricsHandler())
package goruntime

import (
	"context"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	// How often to re-read usage and recompute GOMEMLIMIT. Default 10s.
	Interval time.Duration
	// FastInterval is used instead of Interval while the other containers'
	// memory is rising, so a growing sidecar is caught before it pushes the
	// task over its limit. Default Interval/4.
	FastInterval time.Duration
	// PeakWindow: GOMEMLIMIT is sized from the other containers' peak memory
	// use over this window. It drops as soon as they grow, but rises again
	// only once their usage has stayed lower for the whole window.
	// Default 2m. Negative follows their latest reading instead.
	PeakWindow time.Duration
	Policy     MemLimitPolicy
	// GCPercent applied once a memory limit is known. The big CPU savings come
	// from letting GOMEMLIMIT, not GOGC, decide when to collect, so the default
	// is -1 (GOGC=off). Set e.g. 100 to keep GOGC as a second trigger.
	// Ignored if the GOGC env var is set.
	GCPercent *int
	// Skip setting GOMAXPROCS / GOMEMLIMIT (metrics still work).
	DisableMaxProcs bool
	DisableMemLimit bool
	Logger          *slog.Logger

	// For tests: override where limits come from, and the clock.
	snapshotFn func(context.Context) (Snapshot, error)
	now        func() time.Time
}

type Runtime struct {
	cfg  Config
	log  *slog.Logger
	snap func(context.Context) (Snapshot, error)

	mu         sync.RWMutex
	last       Snapshot
	memLimit   uint64
	maxProcs   int
	cpuLimit   float64
	updates    atomic.Uint64
	errors     atomic.Uint64
	gcDisabled bool

	peak       peakWindow
	othersPeak uint64 // peak other-container usage GOMEMLIMIT was sized from
	prevOthers uint64
	haveOthers bool

	stop chan struct{}
	done chan struct{}
}

// Start detects limits, applies GOMAXPROCS and GOMEMLIMIT, and keeps
// GOMEMLIMIT updated in the background until ctx is cancelled or Stop is
// called. It never fails hard: if no limits are found, the runtime is left
// at Go's defaults and only metrics are served.
func Start(ctx context.Context, cfg Config) (*Runtime, error) {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.FastInterval <= 0 || cfg.FastInterval > cfg.Interval {
		cfg.FastInterval = cfg.Interval / 4
	}
	if cfg.PeakWindow == 0 {
		cfg.PeakWindow = 2 * time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	r := &Runtime{
		cfg:  cfg,
		log:  cfg.Logger.With("component", "goruntime"),
		peak: peakWindow{window: cfg.PeakWindow},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	r.snap = cfg.snapshotFn
	if r.snap == nil {
		r.snap = defaultSnapshot()
	}

	// Respect explicit env overrides, like the Go runtime does.
	if os.Getenv("GOMEMLIMIT") != "" {
		r.log.Info("GOMEMLIMIT set in env; dynamic tuning disabled", "GOMEMLIMIT", os.Getenv("GOMEMLIMIT"))
		r.cfg.DisableMemLimit = true
	}
	if os.Getenv("GOMAXPROCS") != "" {
		r.cfg.DisableMaxProcs = true
	}

	s, err := r.snap(ctx)
	if err != nil {
		r.errors.Add(1)
		r.log.Warn("could not read limits", "err", err)
	}
	next := r.apply(s, true)

	go r.loop(ctx, next)
	return r, nil
}

func defaultSnapshot() func(context.Context) (Snapshot, error) {
	if c := newECSClientFromEnv(); c != nil {
		return c.snapshot
	}
	return func(context.Context) (Snapshot, error) {
		s, _ := cgroupSnapshot()
		return s, nil
	}
}

func (r *Runtime) loop(ctx context.Context, next time.Duration) {
	defer close(r.done)
	t := time.NewTimer(next)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, r.cfg.Interval)
			s, err := r.snap(cctx)
			cancel()
			if err != nil {
				r.errors.Add(1)
				r.log.Warn("could not refresh limits", "err", err)
				next = r.cfg.Interval // keep the last good limit
			} else {
				next = r.apply(s, false)
			}
			t.Reset(next)
		}
	}
}

// apply records s, updates the limits, and returns how long to wait before
// the next refresh.
func (r *Runtime) apply(s Snapshot, first bool) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last = s

	// Track the other containers' usage: its peak sizes GOMEMLIMIT, and a
	// rise of 1% of the task limit or more switches to fast polling.
	others := s.othersMemoryUsage()
	r.othersPeak = r.peak.add(r.cfg.now(), others)
	rising := r.haveOthers && others > r.prevOthers &&
		others-r.prevOthers >= s.TaskMemoryLimit/100
	r.prevOthers, r.haveOthers = others, true
	next := r.cfg.Interval
	if rising && s.TaskMemoryLimit > 0 {
		next = r.cfg.FastInterval
	}

	if first {
		r.cpuLimit = s.CPULimit()
		if !r.cfg.DisableMaxProcs {
			n, set := applyMaxProcs(r.cpuLimit)
			r.maxProcs = n
			if set {
				r.log.Info("set GOMAXPROCS", "value", n, "cpu_limit", r.cpuLimit, "source", s.Source)
			}
		} else {
			r.maxProcs, _ = applyMaxProcsNoop()
		}
	}

	if r.cfg.DisableMemLimit {
		r.memLimit = uint64(debug.SetMemoryLimit(-1))
		return next
	}
	sized := s
	if s.TaskMemoryLimit > 0 {
		sized.TaskMemoryUsage = s.ContainerMemoryUsage + r.othersPeak
	}
	want := ComputeMemLimit(sized, r.cfg.Policy)
	if want == 0 {
		r.memLimit = uint64(debug.SetMemoryLimit(-1))
		return next
	}
	// Skip tiny changes to avoid churning the GC pacer.
	if r.memLimit != 0 && absDiff(want, r.memLimit) < r.memLimit/100 {
		return next
	}
	debug.SetMemoryLimit(int64(want))
	r.updates.Add(1)
	r.log.Info("set GOMEMLIMIT",
		"bytes", want, "mib", want/mib,
		"container_limit_mib", s.ContainerMemoryLimit/mib,
		"task_limit_mib", s.TaskMemoryLimit/mib,
		"task_usage_mib", s.TaskMemoryUsage/mib,
		"others_mib", others/mib,
		"others_peak_mib", r.othersPeak/mib,
		"source", s.Source)
	r.memLimit = want

	if !r.gcDisabled && os.Getenv("GOGC") == "" {
		pct := -1
		if r.cfg.GCPercent != nil {
			pct = *r.cfg.GCPercent
		}
		debug.SetGCPercent(pct)
		r.gcDisabled = true
		r.log.Info("set GOGC", "value", pct)
	}
	return next
}

func applyMaxProcsNoop() (int, bool) { return applyMaxProcs(0) }

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

// Stop halts background tuning. The last applied limits stay in effect.
func (r *Runtime) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	<-r.done
}

// State is a read-only view of what the library has applied.
type State struct {
	Snapshot      Snapshot
	MemoryLimit   uint64
	GOMAXPROCS    int
	CPULimit      float64
	LimitUpdates  uint64
	RefreshErrors uint64
	// Peak memory use of the other containers in the task over PeakWindow;
	// GOMEMLIMIT is sized from this rather than their latest reading.
	OthersMemoryPeak uint64
}

func (r *Runtime) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ml := r.memLimit
	if ml == math.MaxInt64 {
		ml = 0
	}
	return State{
		Snapshot:      r.last,
		MemoryLimit:   ml,
		GOMAXPROCS:    r.maxProcs,
		CPULimit:      r.cpuLimit,
		LimitUpdates:  r.updates.Load(),
		RefreshErrors: r.errors.Load(),

		OthersMemoryPeak: r.othersPeak,
	}
}
