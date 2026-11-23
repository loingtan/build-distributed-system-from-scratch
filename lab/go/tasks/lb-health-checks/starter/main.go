// Health checks. Read the brief: npm run lab -- brief lb-health-checks
//
// Least outstanding requests from the first subtrack. It knows nothing about
// failures: a request sent to a dead backend times out and the client gets an
// error, and a backend that fails fast looks like the least busy of all.
package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"quorumlab"
)

const backendTimeout = 250 * time.Millisecond // healthy backends answer within about 100ms

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

	// TODO: keep track of which backends are healthy.
	//   - every 200ms or so (n.Every, started in n.OnStart), send
	//     {"type": "health"} to every backend
	//   - {"type": "health_ok"} means healthy; no answer, or an error, is a failure
	//   - two failed probes in a row take a backend out of service, one good probe
	//     puts it back
	healthy := map[string]bool{} // backend id -> true while in service

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
			if _, ok := healthy[id]; !ok {
				healthy[id] = true
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_backends_ok"})
	})

	// TODO: choose only among healthy backends you have not tried yet for this
	// request. (Call it with mu held.)
	pick := func() string {
		var best []string
		fewest := math.MaxInt
		for _, id := range backends {
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

	// TODO: if the answer is not get_ok (a timeout or an error), retry once on
	// another backend before giving up.
	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		backend := pick()
		if backend == "" {
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		inFlight[backend]++
		mu.Unlock()

		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)

		mu.Lock()
		inFlight[backend]--
		mu.Unlock()
		if err == nil && res.Type() == "get_ok" {
			return relay(n, msg, res)
		}
		return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
