// Synchronous replication: write_ok only once every backup has confirmed the
// write, and an honest error when one cannot be reached in time.
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
	seq := 0

	// The caller holds mu.
	apply := func(key string, value any, s int) {
		if current, ok := store[key]; !ok || s > current.seq {
			store[key] = version{value, s}
		}
	}

	// replicate keeps sending one write to one backup until it confirms, or
	// time runs out.
	replicate := func(ctx context.Context, peer string, body map[string]any) bool {
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if _, err := n.Call(ctx, peer, body, 60*time.Millisecond); err == nil {
				return true
			}
		}
		return false
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
			// The primary answers within about 500ms either way; relay what it says.
			res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 800*time.Millisecond)
			if err != nil || res.Type() != "write_ok" {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			return n.Reply(msg, map[string]any{"type": "write_ok"})
		}

		mu.Lock()
		seq++
		s := seq
		apply(req.Key, req.Value, s)
		mu.Unlock()

		// Synchronous replication: write_ok means "every node has it", so wait
		// for every backup before saying so. Ask them all at once.
		body := map[string]any{"type": "replicate", "key": req.Key, "value": req.Value, "seq": s}
		peers := n.Peers()
		confirmed := make([]bool, len(peers))
		var wg sync.WaitGroup
		for i, peer := range peers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				confirmed[i] = replicate(ctx, peer, body)
			}()
		}
		wg.Wait()
		for _, ok := range confirmed {
			if !ok {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
		}
		return n.Reply(msg, map[string]any{"type": "write_ok"})
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
		// A retry can deliver the same write twice, or an old one late.
		// apply makes both harmless. Confirm only after applying.
		mu.Lock()
		apply(req.Key, req.Value, req.Seq)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "replicate_ok"})
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
