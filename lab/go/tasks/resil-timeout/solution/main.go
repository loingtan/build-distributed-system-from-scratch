// Timeouts and deadlines: every client request gets one reply within 300ms.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"quorumlab"
)

// Longer than the upstream's slowest normal answer (about 175ms seen from
// here), short enough that the reply still reaches the client inside 300ms.
const upstreamTimeout = 200 * time.Millisecond

func main() {
	n := quorumlab.NewNode()

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)
		if err != nil {
			// Nothing came back. We do not know whether the upstream is slow or
			// dead, only that the client should not wait any longer.
			return n.Reply(msg, map[string]any{
				"type": "error", "code": "timeout",
				"text": fmt.Sprintf("upstream did not answer in %dms", upstreamTimeout.Milliseconds()),
			})
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
