// Monotonic reads: asynchronous primary-backup with session tokens. Every
// node applies the primary's writes strictly in order, so one number, how
// many writes it has applied, describes its state; that number is the token.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type entry struct {
	key   string
	value any
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
	store := map[string]any{} // key -> value
	applied := 0              // writes applied, in the primary's order
	early := map[int]entry{}  // seq -> write that arrived before an earlier one

	// deliver applies a write once every earlier write is applied. The caller holds mu.
	deliver := func(seq int, key string, value any) {
		if seq <= applied {
			return
		}
		early[seq] = entry{key, value}
		for {
			e, ok := early[applied+1]
			if !ok {
				return
			}
			delete(early, applied+1)
			store[e.key] = e.value
			applied++
		}
	}

	// catchUp waits until this node has applied need writes, or gives up.
	catchUp := func(need int, limit time.Duration) bool {
		deadline := time.Now().Add(limit)
		for {
			mu.Lock()
			ok := applied >= need
			mu.Unlock()
			if ok {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(5 * time.Millisecond)
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
			// Pass the write on and relay the answer, token included.
			res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 500*time.Millisecond)
			if err != nil || res.Type() != "write_ok" {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			var ok struct {
				Token any `json:"token"`
			}
			if err := res.Decode(&ok); err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "write_ok", "token": ok.Token})
		}

		mu.Lock()
		seq := applied + 1
		deliver(seq, req.Key, req.Value)
		mu.Unlock()
		// The token covers the write itself, so the client will read its own write.
		if err := n.Reply(msg, map[string]any{"type": "write_ok", "token": seq}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "seq": seq, "key": req.Key, "value": req.Value})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Seq   int    `json:"seq"`
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		deliver(req.Seq, req.Key, req.Value)
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string   `json:"key"`
			Token *float64 `json:"token"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		need := 0
		if req.Token != nil {
			need = int(*req.Token)
		}
		// Behind what this client has already seen? Wait for replication to catch up.
		if !catchUp(need, 600*time.Millisecond) {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		token := applied
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value, "token": token})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
