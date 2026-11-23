// Observed-remove set. Read the brief: npm run lab -- brief crdt-or-set
//
// Your grow-only set, with a remove that deletes the element locally.
// Run the tests: the next merge from a peer that still has the element puts
// it straight back. A union cannot tell "never had it" from "removed it".
// TODO: make removes stick, let an element be added again, and let an add
// win over a remove that could not have seen it.
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

	n.Handle("remove", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Element string `json:"element"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		delete(elements, req.Element)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "remove_ok"})
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
