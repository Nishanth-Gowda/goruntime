package goruntime

// Snapshot is one reading of the resource limits and usage around this process.
// Memory values are bytes; CPU values are in cores (vCPUs).
type Snapshot struct {
	Source string // "ecs", "cgroup", or "" if nothing was found

	TaskMemoryLimit uint64
	TaskMemoryUsage uint64 // sum of working sets of every container in the task
	TaskCPULimit    float64

	ContainerMemoryLimit uint64 // 0 = not set (container can use up to the task limit)
	ContainerMemoryUsage uint64
	ContainerCPULimit    float64
}

// MemLimitPolicy controls how GOMEMLIMIT is derived from a Snapshot.
type MemLimitPolicy struct {
	// Ratio of the available memory to hand to the Go runtime. The rest is
	// headroom for non-heap memory (goroutine stacks are counted by the
	// runtime, but cgo, mmap'd files, page cache, etc. are not). Default 0.9.
	Ratio float64
	// MinRatio is a floor, as a fraction of this container's hard limit, so a
	// greedy sidecar can't push GOMEMLIMIT so low the GC runs non-stop.
	// Default 0.5.
	MinRatio float64
}

func (p MemLimitPolicy) withDefaults() MemLimitPolicy {
	if p.Ratio <= 0 || p.Ratio > 1 {
		p.Ratio = 0.9
	}
	if p.MinRatio <= 0 || p.MinRatio > p.Ratio {
		p.MinRatio = 0.5
		if p.MinRatio > p.Ratio {
			p.MinRatio = p.Ratio
		}
	}
	return p
}

// ComputeMemLimit returns the GOMEMLIMIT to set, or 0 if no limit is known.
//
// The hard ceiling for this container is the smaller of:
//   - its own container memory limit (if set), and
//   - the task memory limit minus what the *other* containers in the task
//     are using right now (sidecars like envoy / log routers share the task
//     limit, and the whole task gets OOM-killed if the sum crosses it).
//
// GOMEMLIMIT is Ratio of that ceiling, but never below MinRatio of the
// container's static ceiling.
func ComputeMemLimit(s Snapshot, p MemLimitPolicy) uint64 {
	p = p.withDefaults()

	static := s.ContainerMemoryLimit
	if static == 0 || (s.TaskMemoryLimit > 0 && s.TaskMemoryLimit < static) {
		static = s.TaskMemoryLimit
	}
	if static == 0 {
		return 0
	}

	ceiling := static
	if s.TaskMemoryLimit > 0 {
		others := uint64(0)
		if s.TaskMemoryUsage > s.ContainerMemoryUsage {
			others = s.TaskMemoryUsage - s.ContainerMemoryUsage
		}
		if others < s.TaskMemoryLimit {
			if avail := s.TaskMemoryLimit - others; avail < ceiling {
				ceiling = avail
			}
		} else {
			ceiling = 0
		}
	}

	limit := uint64(float64(ceiling) * p.Ratio)
	floor := uint64(float64(static) * p.MinRatio)
	if limit < floor {
		limit = floor
	}
	return limit
}

// CPULimit returns the CPU hard limit to size GOMAXPROCS from. The task-level
// limit is preferred (that is what CFS throttles on in ECS); the container
// value is used when the task has none.
func (s Snapshot) CPULimit() float64 {
	if s.TaskCPULimit > 0 {
		return s.TaskCPULimit
	}
	return s.ContainerCPULimit
}
