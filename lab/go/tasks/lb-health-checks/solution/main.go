// Health checks: probe every backend, take failed ones out of service, put
// them back when they answer again, and retry a failed forward elsewhere.
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
	backendTimeout = 250 * time.Millisecond // healthy backends answer within about 100ms
	probeEvery     = 200 * time.Millisecond
	probeTimeout   = 150 * time.Millisecond
	downAfter      = 2 // failed probes in a row that take a backend out
	maxAttempts    = 2 // the first try and one retry elsewhere
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
	var backends []string        // the pool, from the latest set_backends
	inFlight := map[string]int{} // backend id -> requests in flight
	healthy := map[string]bool{} // backend id -> true while in service
	failures := map[string]int{} // backend id -> failed probes in a row
	known := map[string]bool{}   // backends we have set up state for

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
				healthy[id] = true // innocent until a probe says otherwise
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_backends_ok"})
	})

	// Probe every backend, in service or not: a probe is the only way a backend
	// that was taken out can come back.
	probe := func(ctx context.Context, id string) {
		res, err := n.Call(ctx, id, map[string]any{"type": "health"}, probeTimeout)
		mu.Lock()
		defer mu.Unlock()
		if err == nil && res.Type() == "health_ok" {
			failures[id] = 0
			healthy[id] = true
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

	// Fewest requests in flight among the healthy backends not tried yet.
	// Call with mu held.
	pick := func(tried []string) string {
		var best []string
		fewest := math.MaxInt
		for _, id := range backends {
			if !healthy[id] || slices.Contains(tried, id) {
				continue
			}
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

			// Every attempt gives its slot back, however it ended.
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
