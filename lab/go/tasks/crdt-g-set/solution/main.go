// Grow-only set. Elements are only ever added, so two replicas merge by
// taking the union. Union is commutative, associative and idempotent: send
// the whole set to every peer on a timer, and a lost, late or repeated
// message does no harm. The next round repairs whatever was missed.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	elements := map[string]bool{}

	list := func() []string {
		out := make([]string, 0, len(elements))
		for e := range elements {
			out = append(out, e)
		}
		return out
	}

	n.OnStart(func(ctx context.Context) {
		// Anti-entropy: every 100ms, every peer gets the full state.
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"type": "merge", "elements": list()})
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, json.RawMessage(body))
			}
		})
	})

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Element string `json:"element"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		elements[req.Element] = true
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := list()
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Elements []string `json:"elements"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for _, e := range req.Elements {
			elements[e] = true
		}
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
