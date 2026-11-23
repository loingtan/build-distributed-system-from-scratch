// Rate limiting with a token bucket: 20 per second, bursts of 10.
package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"quorumlab"
)

const (
	upstreamTimeout = 200 * time.Millisecond
	rate            = 20 // tokens added per second
	burst           = 10 // most tokens the bucket can hold
)

type bucket struct {
	mu         sync.Mutex
	tokens     float64
	refilledAt time.Duration
}

// take needs no timer: it works out how many tokens have dripped in since the
// last look. Fractions are kept, so 30ms of waiting is not thrown away.
func (b *bucket) take(now time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = math.Min(burst, b.tokens+(now-b.refilledAt).Seconds()*rate)
	b.refilledAt = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
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
			// Refusing is cheap and immediate; the upstream never hears of it.
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
