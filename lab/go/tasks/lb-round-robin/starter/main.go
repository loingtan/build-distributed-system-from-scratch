// Round robin. Read the brief: npm run lab -- brief lb-round-robin
//
// A load balancer that forwards every request and relays the answer.
// It works, but it sends everything to the first backend in the pool.
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

	// TODO: give the backends turns: the first request to backends[0], the next
	// to backends[1], and so on, starting over after the last one. The pool can
	// change size between two requests. (Call it with mu held.)
	pick := func() string {
		return backends[0]
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
