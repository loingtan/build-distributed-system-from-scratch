// Retries with backoff and jitter: retry transient failures, at most 4 calls.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"quorumlab"
)

const (
	upstreamTimeout = 200 * time.Millisecond
	maxAttempts     = 4                      // one first try plus three retries
	baseWait        = 100 * time.Millisecond // ceiling of the wait before the first retry
)

type fetchReply struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
	Code  any    `json:"code"`
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func main() {
	n := quorumlab.NewNode()

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if attempt > 1 {
				// Exponential backoff with full jitter: the ceiling doubles after
				// every failure (100, 200, 400ms) and the actual wait is a random
				// share of it, so requests that failed together do not return together.
				ceiling := baseWait << (attempt - 2)
				sleep(ctx, time.Duration(n.Rand()*float64(ceiling)))
			}
			res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)
			if err != nil {
				continue // silence may pass
			}
			var up fetchReply
			if err := res.Decode(&up); err != nil {
				return err
			}
			if up.Type == "fetch_ok" {
				return n.Reply(msg, map[string]any{"type": "get_ok", "value": up.Value})
			}
			// "busy" may pass. Anything else will be the same next time.
			if up.Code != "busy" {
				return n.Reply(msg, map[string]any{"type": "error", "code": up.Code})
			}
		}
		// Worst case: 4 timeouts of 200ms plus waits of at most 700ms.
		return n.Reply(msg, map[string]any{
			"type": "error", "code": "unavailable",
			"text": fmt.Sprintf("gave up after %d attempts", maxAttempts),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
