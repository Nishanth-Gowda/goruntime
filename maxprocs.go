package goruntime

import (
	"math"
	"runtime"
)

// ComputeMaxProcs turns a CPU quota (in cores) into a GOMAXPROCS value.
// It rounds down so the process never schedules more parallel work than the
// quota allows (which is what causes CFS throttling), with a minimum of 1 and
// a maximum of the host's CPU count. Returns 0 when there is no quota.
func ComputeMaxProcs(cpuLimit float64, numCPU int) int {
	if cpuLimit <= 0 {
		return 0
	}
	n := int(math.Floor(cpuLimit))
	if n < 1 {
		n = 1
	}
	if numCPU > 0 && n > numCPU {
		n = numCPU
	}
	return n
}

func applyMaxProcs(cpuLimit float64) (int, bool) {
	n := ComputeMaxProcs(cpuLimit, runtime.NumCPU())
	if n == 0 {
		return runtime.GOMAXPROCS(0), false
	}
	runtime.GOMAXPROCS(n)
	return n, true
}
