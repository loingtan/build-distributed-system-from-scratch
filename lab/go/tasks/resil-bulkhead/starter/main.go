// Bulkhead: bounded concurrency. Read the brief: npm run lab -- brief resil-bulkhead
//
// The plain service from the first task. When the upstream slows down,
// requests pile up inside this node, each one holding an upstream call open.
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
	_ = &mu
	_ = inFlight

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: if maxInFlight calls are already out, refuse at once with
		// {"type":"error","code":"overloaded"}. Otherwise count this call in, and
		// count it out again when it ends: on a reply, an error or a timeout.
		// Handlers run concurrently: check and count under mu, but do not hold
		// it across n.Call.
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
