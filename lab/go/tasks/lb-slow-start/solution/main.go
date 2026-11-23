// Slow start: a backend that comes into service ramps from no traffic to a
// full share over three seconds, instead of being flooded while it is cold.
package main

import (
	"context"
	"log"
	"math"
	"slices"
	"sync"
	"time"

	"quorumlab"
)

const (
	backendTimeout = 250 * time.Millisecond
	probeEvery     = 200 * time.Millisecond
	probeTimeout   = 150 * time.Millisecond
	downAfter      = 2
	maxAttempts    = 2
	slowStart      = 3000 * time.Millisecond // for a backend to ramp from nothing to a full share
)

// relay passes a backend's answer back to the client unchanged (only the
// backend's own message id is dropped; Reply adds in_reply_to).
func relay(n *quorumlab.Node, req, res quorumlab.Message) error {
	var body map[string]any
	if err := res.Decode(&body); err != nil {
		return err
	}
	delete(body, "msg_id")
	delete(body, "in_reply_to")
	return n.Reply(req, body)
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var backends []string
	firstList := true // the very first pool is warm
	inFlight := map[string]int{}
	healthy := map[string]bool{}
	failures := map[string]int{}
	known := map[string]bool{}
	upSince := map[string]time.Duration{} // backend id -> when it came into service; absent for warm backends

	n.Handle("set_backends", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Backends []string `json:"backends"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		backends = req.Backends
		for _, id := range backends {
			if !known[id] {
				known[id] = true
				healthy[id] = true
				if !firstList {
					upSince[id] = n.Elapsed() // a new backend starts cold
				}
			}
		}
		firstList = false
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_backends_ok"})
	})

	probe := func(ctx context.Context, id string) {
		res, err := n.Call(ctx, id, map[string]any{"type": "health"}, probeTimeout)
		mu.Lock()
		defer mu.Unlock()
		if err == nil && res.Type() == "health_ok" {
			failures[id] = 0
			if !healthy[id] {
				healthy[id] = true
				upSince[id] = n.Elapsed() // back from an outage: cold again
			}
		} else {
			failures[id]++
			if failures[id] >= downAfter {
				healthy[id] = false
			}
		}
	}

	n.OnStart(func(ctx context.Context) {
		n.Every(probeEvery, func(ctx context.Context) {
			mu.Lock()
			pool := slices.Clone(backends)
			mu.Unlock()
			for _, id := range pool {
				go probe(ctx, id)
			}
		})
	})

	// 0 when a backend has just come into service, 1 from slowStart later.
	weight := func(id string, now time.Duration) float64 {
		since, ok := upSince[id]
		if !ok {
			return 1
		}
		return math.Min(1, float64(now-since)/float64(slowStart))
	}

	leastInFlight := func(ids []string) string {
		var best []string
		fewest := math.MaxInt
		for _, id := range ids {
			switch c := inFlight[id]; {
			case c < fewest:
				fewest = c
				best = []string{id}
			case c == fewest:
				best = append(best, id)
			}
		}
		if len(best) == 0 {
			return ""
		}
		return best[int(n.Rand()*float64(len(best)))]
	}

	// Two steps. A warming backend is idle and fast, so least-outstanding alone
	// would hand it a full share at once. First give each warming backend this
	// request with probability weight / total weight; otherwise choose among the
	// warm backends by fewest requests in flight. Call with mu held.
	pick := func(tried []string) string {
		now := n.Elapsed()
		var warm, warming []string
		total := 0.0
		for _, id := range backends {
			if !healthy[id] || slices.Contains(tried, id) {
				continue
			}
			w := weight(id, now)
			total += w
			if w >= 1 {
				warm = append(warm, id)
			} else {
				warming = append(warming, id)
			}
		}
		if len(warm)+len(warming) == 0 {
			return ""
		}
		r := n.Rand() * total
		for _, id := range warming {
			w := weight(id, now)
			if r < w {
				return id
			}
			r -= w
		}
		if len(warm) > 0 {
			return leastInFlight(warm)
		}
		return leastInFlight(warming)
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		var tried []string
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			mu.Lock()
			backend := pick(tried)
			if backend == "" {
				mu.Unlock()
				break
			}
			tried = append(tried, backend)
			inFlight[backend]++
			mu.Unlock()

			res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)

			mu.Lock()
			inFlight[backend]--
			mu.Unlock()
			if err == nil && res.Type() == "get_ok" {
				return relay(n, msg, res)
			}
		}
		return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
