// Cache-aside: look in memory first; on a miss, read the database and keep
// the answer for next time.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const dbTimeout = 1000 * time.Millisecond

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	cache := map[string]any{} // key -> value read from the database

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value, ok := cache[req.Key]
		mu.Unlock()
		if ok {
			// A hit: no database call at all.
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
		}

		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": req.Key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		if err != nil || res.Type() != "read_ok" || res.Decode(&body) != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		mu.Lock()
		cache[req.Key] = body.Value
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
