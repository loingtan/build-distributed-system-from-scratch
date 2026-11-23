// Round robin load balancer: give the backends turns in order.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const backendTimeout = 500 * time.Millisecond

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
	next := 0             // whose turn it is; only ever grows

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

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// Pick and move the counter on under the lock, before the call: other
		// requests run while this one waits, and each must see the next turn.
		mu.Lock()
		if len(backends) == 0 {
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "error", "code": "no_backends"})
		}
		backend := backends[next%len(backends)]
		next++
		mu.Unlock()

		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		// The backend's answer goes back as it is. Waiting inside this handler
		// keeps it paired with this client's request, whatever order answers arrive in.
		return relay(n, msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
