// Grow-only counter as a CRDT: one entry per node, merged with max.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// counts[id] is how much has been added at node id. A node only ever
	// increases its own entry, so for any entry the larger value is the newer
	// one, and merging is safe to repeat in any order.
	var mu sync.Mutex
	counts := map[string]int{}

	n.OnStart(func(ctx context.Context) {
		// Anti-entropy: keep telling peers everything we know. A lost message
		// is repaired by the next round; a duplicate changes nothing.
		n.Every(150*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			snapshot := make(map[string]int, len(counts))
			for k, v := range counts {
				snapshot[k] = v
			}
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, map[string]any{"type": "merge", "counts": snapshot})
			}
		})
	})

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Delta int `json:"delta"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		counts[n.ID()] += req.Delta
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Counts map[string]int `json:"counts"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for id, v := range req.Counts {
			if v > counts[id] {
				counts[id] = v
			}
		}
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		sum := 0
		for _, v := range counts {
			sum += v
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": sum})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
