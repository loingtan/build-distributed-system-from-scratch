// Grow-only set. Read the brief: npm run lab -- brief crdt-g-set
//
// A grow-only set. Each add is stored locally and sent to every peer once.
// Run the tests: a message lost in transit, or dropped by a partition, is
// never sent again, so some replicas never learn about some elements.
// TODO: replace the one-off message with anti-entropy.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	elements := map[string]bool{}

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
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "merge", "elements": []string{req.Element}})
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := make([]string, 0, len(elements))
		for e := range elements {
			value = append(value, e)
		}
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
