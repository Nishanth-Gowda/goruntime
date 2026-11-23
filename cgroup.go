package goruntime

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroup fallback for plain Docker / Kubernetes / anything that isn't ECS.
// Only reads the cgroup the process is in at the root of the mount, which is
// what containers see.

var cgroupRoot = "/sys/fs/cgroup"

func readTrim(path string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func readUint(path string) (uint64, bool) {
	s, ok := readTrim(path)
	if !ok || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

func readStatKey(path, key string) uint64 {
	s, ok := readTrim(path)
	if !ok {
		return 0
	}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == key {
			v, _ := strconv.ParseUint(f[1], 10, 64)
			return v
		}
	}
	return 0
}

func cgroupSnapshot() (Snapshot, bool) {
	s := Snapshot{Source: "cgroup"}
	found := false

	// cgroup v2
	if lim, ok := readUint(filepath.Join(cgroupRoot, "memory.max")); ok {
		s.ContainerMemoryLimit, found = lim, true
	}
	if _, ok := readTrim(filepath.Join(cgroupRoot, "memory.current")); ok {
		u, _ := readUint(filepath.Join(cgroupRoot, "memory.current"))
		inactive := readStatKey(filepath.Join(cgroupRoot, "memory.stat"), "inactive_file")
		if inactive < u {
			u -= inactive
		}
		s.ContainerMemoryUsage = u
	}
	if v, ok := readTrim(filepath.Join(cgroupRoot, "cpu.max")); ok {
		f := strings.Fields(v)
		if len(f) == 2 && f[0] != "max" {
			q, e1 := strconv.ParseFloat(f[0], 64)
			p, e2 := strconv.ParseFloat(f[1], 64)
			if e1 == nil && e2 == nil && p > 0 {
				s.ContainerCPULimit, found = q/p, true
			}
		}
	}

	// cgroup v1
	if !found {
		if lim, ok := readUint(filepath.Join(cgroupRoot, "memory", "memory.limit_in_bytes")); ok && lim < 1<<60 {
			s.ContainerMemoryLimit, found = lim, true
			u, _ := readUint(filepath.Join(cgroupRoot, "memory", "memory.usage_in_bytes"))
			inactive := readStatKey(filepath.Join(cgroupRoot, "memory", "memory.stat"), "total_inactive_file")
			if inactive < u {
				u -= inactive
			}
			s.ContainerMemoryUsage = u
		}
		q, ok1 := readTrim(filepath.Join(cgroupRoot, "cpu", "cpu.cfs_quota_us"))
		p, ok2 := readUint(filepath.Join(cgroupRoot, "cpu", "cpu.cfs_period_us"))
		if ok1 && ok2 && p > 0 {
			if qv, err := strconv.ParseInt(q, 10, 64); err == nil && qv > 0 {
				s.ContainerCPULimit, found = float64(qv)/float64(p), true
			}
		}
	}

	s.TaskMemoryLimit = s.ContainerMemoryLimit
	s.TaskMemoryUsage = s.ContainerMemoryUsage
	s.TaskCPULimit = s.ContainerCPULimit
	return s, found
}
