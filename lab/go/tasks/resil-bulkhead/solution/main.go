// Bulkhead: at most 4 upstream calls in flight; refuse the rest at once.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	upstreamTimeout = 200 * time.Millisecond
	maxInFlight     = 4 // upstream calls allowed at the same time
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	inFlight := 0 // upstream calls sent and not yet finished

	// acquire takes a slot if one is free. Check and increment happen under one
	// lock, so two concurrent requests cannot both take the last slot.
	acquire := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if inFlight >= maxInFlight {
			return false
		}
		inFlight++
		return true
	}
	release := func() {
		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if !acquire() {
			// No queue: a request that waited would only be answered late, and the
			// queue would hide the overload from the client.
			return n.Reply(msg, map[string]any{"type": "error", "code": "overloaded"})
		}
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)
		// Give the slot back before looking at the result, so that no outcome
		// (reply, error, timeout) can leak it.
		release()
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		var up struct {
			Value any `json:"value"`
		}
		if err := res.Decode(&up); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": up.Value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
