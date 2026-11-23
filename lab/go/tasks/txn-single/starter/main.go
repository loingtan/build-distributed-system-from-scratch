// Single-node transactions. Read the brief: npm run lab -- brief txn-single
package main

import (
	"context"
	"log"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	store := map[any]any{} // key -> value
	_ = store              // Go rejects unused variables; delete this line once you use store

	n.Handle("txn", func(ctx context.Context, msg quorumlab.Message) error {
		// req.Txn is a list like [["r", 1, null], ["w", 1, 6], ["r", 1, null]]
		var req struct {
			Txn [][3]any `json:"txn"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: run each operation in order against store.
		//   ["r", key, null]  -> ["r", key, <current value, or null>]
		//   ["w", key, value] -> store it, return it unchanged
		// Unlike the browser lab, Go handlers run concurrently: two
		// transactions can be handled at the same moment. Guard store with a
		// sync.Mutex held for the whole transaction, so they run one at a time.
		result := [][3]any{}
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": result})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
