// Circuit breaker: closed, open and half-open, one breaker per node.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	upstreamTimeout  = 200 * time.Millisecond
	failureThreshold = 5           // failures in a row that open the breaker
	coolDown         = time.Second // how long to stay open before probing
)

type breaker struct {
	mu       sync.Mutex
	state    string        // "closed" | "open" | "half_open"
	failures int           // consecutive failures while closed
	openedAt time.Duration // n.Elapsed() when the breaker last opened
}

// open must be called with mu held.
func (b *breaker) open(now time.Duration) {
	b.state = "open"
	b.openedAt = now
	b.failures = 0
}

// allow says whether this request may call the upstream, and whether it is
// the probe. Deciding both under one lock means only one request can become
// the probe.
func (b *breaker) allow(now time.Duration) (allowed, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.state == "closed":
		return true, false
	case b.state == "open" && now-b.openedAt >= coolDown:
		// Cool-down over: this request is the single probe.
		b.state = "half_open"
		return true, true
	default:
		return false, false // open, or half-open with the probe still in flight
	}
}

func (b *breaker) record(now time.Duration, success, wasProbe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if wasProbe {
		// The probe alone decides: closed again, or another full cool-down.
		if success {
			b.state = "closed"
			b.failures = 0
		} else {
			b.open(now)
		}
		return
	}
	// Results of calls sent before the breaker opened still trickle in. They
	// say nothing new, so they are ignored unless the breaker is closed.
	if b.state != "closed" {
		return
	}
	if success {
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= failureThreshold {
		b.open(now)
	}
}

func main() {
	n := quorumlab.NewNode()
	b := &breaker{state: "closed"}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		ok, wasProbe := b.allow(n.Elapsed())
		if !ok {
			return n.Reply(msg, map[string]any{"type": "error", "code": "circuit_open"})
		}
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)
		var up struct {
			Type  string `json:"type"`
			Value any    `json:"value"`
			Code  any    `json:"code"`
		}
		if err == nil {
			if derr := res.Decode(&up); derr != nil {
				return derr
			}
		}
		success := err == nil && up.Type == "fetch_ok"
		b.record(n.Elapsed(), success, wasProbe)
		switch {
		case success:
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": up.Value})
		case err != nil:
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		default:
			return n.Reply(msg, map[string]any{"type": "error", "code": up.Code})
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
