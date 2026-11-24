package goruntime

import "time"

// peakWindow tracks the maximum of a value over a sliding time window.
//
// GOMEMLIMIT is sized from the *peak* memory use of the other containers in
// the task over the window, not their latest reading. A sidecar that grows
// lowers GOMEMLIMIT on the very next refresh (the new reading is the new
// peak), but one that shrinks only gives the memory back once its high
// reading has aged out of the window. This keeps a sidecar whose usage swings
// from dragging GOMEMLIMIT up and down (which unsettles the GC pacer), and
// keeps headroom for it to spike again.
//
// It is a monotonic queue: readings are kept in time order with values
// strictly decreasing, so the front is always the peak and each reading is
// pushed and popped at most once.
type peakWindow struct {
	window time.Duration // <= 0: no smoothing, add returns the latest value
	q      []peakSample
}

type peakSample struct {
	at time.Time
	v  uint64
}

// add records v at time t and returns the peak over (t-window, t].
func (p *peakWindow) add(t time.Time, v uint64) uint64 {
	if p.window <= 0 {
		return v
	}
	// Older readings that are <= v can never be the peak again.
	for len(p.q) > 0 && p.q[len(p.q)-1].v <= v {
		p.q = p.q[:len(p.q)-1]
	}
	p.q = append(p.q, peakSample{at: t, v: v})
	// Drop expired readings from the front; the newest one always stays.
	cut := t.Add(-p.window)
	i := 0
	for i < len(p.q)-1 && !p.q[i].at.After(cut) {
		i++
	}
	p.q = p.q[i:]
	return p.q[0].v
}
