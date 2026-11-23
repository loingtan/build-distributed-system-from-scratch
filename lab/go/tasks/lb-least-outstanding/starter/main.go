// Least outstanding requests. Read the brief: npm run lab -- brief lb-least-outstanding
//
// Round robin from the previous task. It shares requests evenly, but some
// backends are much slower than others: an equal share of requests to a slow
// backend means a quarter of all clients wait on it while the others idle.
package main

import (
	"context"
	"log"
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
	var backends []string // the pool, from the latest set_backends
	next := 0

	// TODO: keep, for every backend, the number of requests sent to it that have
	// not finished yet, and pick the backend where that number is smallest.
	inFlight := map[string]int{} // backend id -> requests in flight
	_ = inFlight

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

	// Call with mu held.
	pick := func() string {
		backend := backends[next%len(backends)]
		next++
		return backend
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
		mu.Unlock()

		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		return relay(n, msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
