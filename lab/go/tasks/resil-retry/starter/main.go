// Retries with backoff and jitter. Read the brief: npm run lab -- brief resil-retry
//
// One attempt per request, with a timeout. A single lost or refused call
// now fails the whole request.
package main

import (
	"context"
	"log"
	"time"

	"quorumlab"
)

const (
	upstreamTimeout = 200 * time.Millisecond
	maxAttempts     = 4                      // one first try plus three retries
	baseWait        = 100 * time.Millisecond // ceiling of the wait before the first retry
)

// fetchReply is what the upstream answers: fetch_ok with a value, or an error with a code.
type fetchReply struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
	Code  any    `json:"code"`
}

// sleep pauses this request without blocking the node (handlers run in their
// own goroutines). It returns early if ctx is cancelled.
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
		res, err := n.Call(ctx, "upstream", map[string]any{"type": "fetch", "key": req.Key}, upstreamTimeout)

		// TODO: three outcomes need three different reactions.
		//   err != nil, or code "busy"         transient: wait, then try again,
		//                                      up to maxAttempts calls in all
		//   any other error (e.g. "not_found") permanent: pass the code on
		//   type "fetch_ok"                    done
		// The wait before retry k is a random share of baseWait * 2^(k-1):
		//   time.Duration(n.Rand() * float64(baseWait << (k-1)))
		// When every attempt has failed, reply {"type":"error","code":"unavailable"}.
		_ = sleep
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		var up fetchReply
		if err := res.Decode(&up); err != nil {
			return err
		}
		if up.Type == "error" {
			return n.Reply(msg, map[string]any{"type": "error", "code": up.Code})
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": up.Value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
