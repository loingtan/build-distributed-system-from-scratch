// Asynchronous replication: the primary numbers its writes, answers at once
// and then tells the backups, which apply writes by number.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// version is a value and the position of its write in the primary's order;
// seq is what lets a backup tell an old write from a new one.
type version struct {
	value any
	seq   int
}

func main() {
	n := quorumlab.NewNode()

	// The first node is the primary; it never changes. Node ids are known once
	// the node has started.
	var primary string
	var isPrimary bool
	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary
	})

	var mu sync.Mutex
	store := map[string]version{} // key -> {value, seq}
	seq := 0                      // used by the primary only

	// apply keeps a write unless this node already holds a newer one for the
	// key. The caller holds mu.
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
			// Backups do not decide anything. Pass the write on and relay the answer.
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
		// Asynchronous replication: answer first, tell the backups afterwards.
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
		// Messages can overtake each other; apply drops the ones that are late.
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
