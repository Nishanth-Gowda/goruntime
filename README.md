# goruntime

A Go take on the runtime library from Zomato's post
["Go Beyond: Building Performant and Reliable Golang Applications"](https://www.zomato.com/blog/go-beyond-building-performant-and-reliable-golang-applications/).
It has no dependencies outside the standard library.

| Article feature | Where |
|---|---|
| Dynamic GOMEMLIMIT from ECS task + container limits and live usage | `memlimit.go`, `ecs.go`, `runtime.go` |
| GOMAXPROCS from the ECS task CPU hard limit | `maxprocs.go` |
| Metrics for GC, heap, memory, CPU and ECS task limits | `metrics.go` (Prometheus text) |
| Fallback when not on ECS (Docker/K8s cgroup v1/v2) | `cgroup.go` |

## Use it

```go
rt, _ := goruntime.Start(ctx, goruntime.Config{})
defer rt.Stop()
http.Handle("/metrics", rt.MetricsHandler())
```

## How GOMEMLIMIT is computed

Every `Interval` (default 10s) the library reads the ECS Task Metadata v4 endpoint
(`$ECS_CONTAINER_METADATA_URI_V4`, `/task`, `/task/stats`):

```
static   = min(container memory limit, task memory limit)
ceiling  = min(static, task limit − working set of the *other* containers)
GOMEMLIMIT = max(ceiling × 0.9, static × 0.5)
```

- Sidecars such as envoy or log routers share the task's memory. When they grow, the
  Go process's limit shrinks so the task as a whole stays under its OOM line.
- The 0.5 floor stops a greedy sidecar from pushing the limit so low that the GC
  runs all the time.
- **Shrink fast, grow slow.** "Other containers" means their *peak* working set over
  `Config.PeakWindow` (default 2m), not the latest reading. When a sidecar grows, the
  limit drops on the next refresh. When it shrinks, the memory is given back only after
  its high reading leaves the window. A sidecar whose usage swings therefore doesn't
  drag GOMEMLIMIT up and down, and there's room for it to spike again.
- While the other containers' usage is rising (by 1% of the task limit or more between
  refreshes), refreshes run every `Config.FastInterval` (default `Interval/4`), so a
  growing sidecar is caught sooner.
- Changes smaller than 1% are skipped so the GC pacer isn't disturbed.
- Once a limit is known, GOGC is set to **off**. Then GC runs only near the limit
  instead of each time the heap doubles, which is where the CPU savings come from.
  Set `Config.GCPercent` to keep GOGC as a second trigger.
- If `GOMEMLIMIT`, `GOMAXPROCS` or `GOGC` is set in the environment, that value wins.

As the article says, this does **not** fix memory leaks. If the live heap goes over
the hard limit, the process still gets OOM-killed. Go caps GC at about 50% CPU when
it is near the limit, so you get a slowdown rather than a GC death spiral.
Watch `go_gc_heap_live_bytes` against `goruntime_gomemlimit_bytes`.

## Try it locally

```bash
go test -race ./...
```

Benchmark the GC savings (small live heap, big budget):

```bash
go run ./cmd/gcbench -live 32 -limit 512 -dur 5s
```

Fake an ECS task (1 GiB / 2 vCPU, with an envoy sidecar whose usage swings over time) and run the demo service:

```bash
go run ./cmd/fakeecs -period 30s
```

```bash
ECS_CONTAINER_METADATA_URI_V4=http://localhost:9100/v4/app go run ./cmd/demo -interval 2s
```

```bash
curl localhost:8080/state
```

```bash
curl -s localhost:8080/metrics | grep goruntime_
```

The demo also serves `/work?mb=N` to generate garbage and `/debug/pprof/` for profiling.
For the article's next step, PGO, save a CPU profile from `/debug/pprof/profile`
as `default.pgo` in the main package and rebuild.
