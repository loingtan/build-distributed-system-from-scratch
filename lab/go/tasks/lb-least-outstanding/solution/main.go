// Least outstanding requests: send each request to the backend with the
// fewest requests in flight, so a slow backend automatically gets less.
package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"quorumlab"
)

const backendTimeout = 1000 * time.Millisecond

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
	inFlight := map[string]int{} // backend id -> requests sent to it and not yet finished

	n.Handle("set_backends", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Backends []string `json:"backends"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		backends = req.Backends
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_backends_ok"})
	})

	// The backend with the fewest requests in flight. Ties are broken at random:
	// with a fixed order the first backend in the list would win every tie.
	// Call with mu held.
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
		return best[int(n.Rand()*float64(len(best)))]
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		if len(backends) == 0 {
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "error", "code": "no_backends"})
		}
		backend := pick()
		// Counted the moment it is picked, under the same lock, so the next
		// request in a burst already sees it.
		inFlight[backend]++
		mu.Unlock()

		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)

		mu.Lock()
		inFlight[backend]--
		mu.Unlock()
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		return relay(n, msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
