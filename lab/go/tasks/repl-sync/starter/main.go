// Synchronous replication. Read the brief: npm run lab -- brief repl-sync
//
// This is the asynchronous solution from the previous task. It answers
// write_ok before any backup has the write, so a client that writes and then
// reads from another node can miss its own write.
//
// What is missing:
//   - the primary must hear back from every backup before it says write_ok
//     (n.Call instead of n.Send, and a reply from the backup)
//   - a lost message must be sent again
//   - if a backup stays silent, give up after about half a second and answer
//     {"type": "error", "code": "unavailable"}
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// version is a value and the position of its write in the primary's order.
type version struct {
	value any
	seq   int
}

func main() {
	n := quorumlab.NewNode()

	// Node ids are known once the node has started.
	var primary string
	var isPrimary bool
	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary
	})

	var mu sync.Mutex
	store := map[string]version{} // key -> {value, seq}
	seq := 0

	// The caller holds mu.
	apply := func(key string, value any, s int) {
		if current, ok := store[key]; !ok || s > current.seq {
			store[key] = version{value, s}
		}
	}

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}

		if !isPrimary {
			res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 500*time.Millisecond)
			if err != nil {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			return n.Reply(msg, map[string]any{"type": res.Type()})
		}

		mu.Lock()
		seq++
		s := seq
		apply(req.Key, req.Value, s)
		mu.Unlock()
		if err := n.Reply(msg, map[string]any{"type": "write_ok"}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "key": req.Key, "value": req.Value, "seq": s})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
			Seq   int    `json:"seq"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		apply(req.Key, req.Value, req.Seq)
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key].value // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
