package goruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// ECS Task Metadata Endpoint v4.
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4.html
const ecsMetadataEnv = "ECS_CONTAINER_METADATA_URI_V4"

const mib = 1 << 20

type ecsClient struct {
	base string
	hc   *http.Client
}

func newECSClientFromEnv() *ecsClient {
	base := os.Getenv(ecsMetadataEnv)
	if base == "" {
		return nil
	}
	return &ecsClient{base: base, hc: &http.Client{Timeout: 2 * time.Second}}
}

type ecsLimits struct {
	CPU    float64 `json:"CPU"`    // task: vCPUs; container: CPU units (1024 = 1 vCPU)
	Memory int64   `json:"Memory"` // MiB
}

type ecsContainer struct {
	DockerID string    `json:"DockerId"`
	Name     string    `json:"Name"`
	Limits   ecsLimits `json:"Limits"`
}

type ecsTask struct {
	Limits     ecsLimits      `json:"Limits"`
	Containers []ecsContainer `json:"Containers"`
}

// Docker stats shape returned by /task/stats (subset).
type ecsStats struct {
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
}

// workingSet mirrors how `docker stats` / the kernel OOM killer see memory:
// usage minus reclaimable page cache.
func (s *ecsStats) workingSet() uint64 {
	u := s.MemoryStats.Usage
	var cache uint64
	if v, ok := s.MemoryStats.Stats["inactive_file"]; ok { // cgroup v2
		cache = v
	} else if v, ok := s.MemoryStats.Stats["total_inactive_file"]; ok { // cgroup v1
		cache = v
	}
	if cache > u {
		return 0
	}
	return u - cache
}

func (c *ecsClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ecs metadata %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// snapshot fetches limits and usage for this container and the whole task.
func (c *ecsClient) snapshot(ctx context.Context) (Snapshot, error) {
	var self ecsContainer
	if err := c.get(ctx, "", &self); err != nil {
		return Snapshot{}, err
	}
	var task ecsTask
	if err := c.get(ctx, "/task", &task); err != nil {
		return Snapshot{}, err
	}
	var stats map[string]*ecsStats
	if err := c.get(ctx, "/task/stats", &stats); err != nil {
		return Snapshot{}, err
	}

	s := Snapshot{Source: "ecs"}
	s.TaskMemoryLimit = uint64(task.Limits.Memory) * mib
	s.TaskCPULimit = task.Limits.CPU
	for _, ct := range task.Containers {
		if ct.DockerID == self.DockerID {
			s.ContainerMemoryLimit = uint64(ct.Limits.Memory) * mib
			s.ContainerCPULimit = ct.Limits.CPU / 1024
		}
	}
	for id, st := range stats {
		if st == nil {
			continue
		}
		ws := st.workingSet()
		s.TaskMemoryUsage += ws
		if id == self.DockerID {
			s.ContainerMemoryUsage = ws
			// Docker reports the effective cgroup limit; use it if the
			// task definition left the container limit unset.
			if s.ContainerMemoryLimit == 0 && st.MemoryStats.Limit < 1<<60 {
				s.ContainerMemoryLimit = st.MemoryStats.Limit
			}
		}
	}
	return s, nil
}
