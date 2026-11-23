// Asynchronous replication. Read the brief: npm run lab -- brief repl-async
//
// Primary-backup replication. The first node is the primary; it never changes.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Node ids are known once the node has started.
	var primary string
	var isPrimary bool
	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary
	})
	_, _ = primary, isPrimary // Go rejects unused variables; delete this line once you use them

	var mu sync.Mutex
	store := map[string]any{} // key -> value

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: only the primary may accept a write. A backup passes the request on:
		//   res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 500*time.Millisecond)
		// and then answers its own client (err is quorumlab.ErrTimeout if no answer came).
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		if err := n.Reply(msg, map[string]any{"type": "write_ok"}); err != nil {
			return err
		}
		// TODO (primary): after replying, send the write to every backup with
		// n.Send. Do not wait for them.
		return nil
	})

	// TODO: a handler for the message the primary sends to its backups.
	// Two of those messages can arrive in the opposite order to the one they
	// were sent in. How does a backup know which write is the newer one?

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
