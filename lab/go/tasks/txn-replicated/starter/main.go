// Totally-available transactions. Read the brief: npm run lab -- brief txn-replicated
//
// Your single-node solution, now running on three nodes that never talk.
// TODO: replicate writes in the background, and make sure every node
// resolves conflicting writes to the same key the same way.
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
	store := map[any]any{} // key -> value

	// In the browser a handler runs to completion before the next message is
	// looked at, so a transaction is serial by construction. Go handlers run
	// concurrently, so the mutex is held for the whole transaction: one node,
	// one transaction at a time.
	n.Handle("txn", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn [][3]any `json:"txn"` // [["r", key, null], ["w", key, value], ...]
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		result := make([][3]any, len(req.Txn))
		mu.Lock()
		for i, op := range req.Txn {
			f, key, value := op[0], op[1], op[2]
			if f == "r" {
				result[i] = [3]any{"r", key, store[key]} // a missing key reads as nil: null
				continue
			}
			store[key] = value
			result[i] = [3]any{"w", key, value}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": result})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
