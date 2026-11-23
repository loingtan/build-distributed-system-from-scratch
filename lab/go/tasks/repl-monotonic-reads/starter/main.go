// Monotonic reads. Read the brief: npm run lab -- brief repl-monotonic-reads
//
// Asynchronous primary-backup, as in RP-1: the primary numbers its writes,
// answers at once and then tells the backups. Clients now read from any
// replica and carry a token: they send the last token they were given in
// every request, as "token" (null at first).
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

	var primary string
	var isPrimary bool
	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary
	})

	var mu sync.Mutex
	store := map[string]version{} // key -> {value, seq}
	seq := 0                      // used by the primary only

	// TODO: per-key numbers stop a node from going backwards, but they cannot say
	// whether a node has caught up with what a client has seen. Apply the
	// primary's writes strictly in order instead (keep early arrivals aside), so
	// that one number, how many writes this node has applied, describes it.
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
			// TODO: relay the primary's token too.
			return n.Reply(msg, map[string]any{"type": res.Type()})
		}

		mu.Lock()
		seq++
		s := seq
		apply(req.Key, req.Value, s)
		mu.Unlock()
		// TODO: give write_ok a token that covers this write.
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
		// TODO: if this node is behind the request's token, wait (poll in a loop,
		// without holding mu) until it has caught up, or answer
		// {"type": "error", "code": "unavailable"} after about 600ms. Then answer
		// with a token of your own.
		mu.Lock()
		value := store[req.Key].value // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
