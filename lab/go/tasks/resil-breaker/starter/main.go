// Circuit breaker. Read the brief: npm run lab -- brief resil-breaker
//
// One attempt per request, with a timeout (retries are left out of this task
// to keep the code short). During an outage every request still calls the
// upstream and waits the full 200ms to learn what the last hundred learned.
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

// breaker is the breaker's state. Handlers run concurrently, so every method
// takes mu; it is never held across the upstream call.
type breaker struct {
	mu       sync.Mutex
	state    string        // "closed" | "open" | "half_open"
	failures int           // consecutive failures while closed
	openedAt time.Duration // n.Elapsed() when the breaker last opened
}

// TODO: decide whether a request may call the upstream, and whether it is the probe.
//
//	closed     yes
//	open       no, unless coolDown has passed: then become half_open and
//	           let this one request through as the probe
//	half_open  no (the probe is already on its way)
func (b *breaker) allow(now time.Duration) (allowed, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return true, false
}

// TODO: record the result of a call.
//
//	success    reset the failure count; a successful probe closes the breaker
//	failure    count it; failureThreshold in a row, or a failed probe,
//	           opens the breaker and sets openedAt
func (b *breaker) record(now time.Duration, success, wasProbe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
			res.Decode(&up)
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
