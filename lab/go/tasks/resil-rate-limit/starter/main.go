// Rate limiting with a token bucket. Read the brief: npm run lab -- brief resil-rate-limit
//
// The plain service from the first task: every request becomes an upstream
// call. Nothing stops a burst of clients from handing the upstream more than
// it can take.
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
	rate            = 20 // tokens added per second
	burst           = 10 // most tokens the bucket can hold
)

// bucket is shared by every handler, so it has its own mutex.
type bucket struct {
	mu         sync.Mutex
	tokens     float64       // the bucket starts full
	refilledAt time.Duration // when the token count was last brought up to date
}

// TODO: bring the token count up to date (elapsed seconds * rate, never
// above burst), then take one if there is a whole token and return true.
// Otherwise return false.
func (b *bucket) take(now time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return true
}

func main() {
	n := quorumlab.NewNode()
	b := &bucket{tokens: burst}

	n.OnStart(func(ctx context.Context) { b.refilledAt = n.Elapsed() })

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if !b.take(n.Elapsed()) {
			return n.Reply(msg, map[string]any{"type": "error", "code": "rate_limited"})
		}
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)
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
