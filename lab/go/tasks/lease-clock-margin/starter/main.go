// Stop before the lease ends. Read the brief: npm run lab -- brief lease-clock-margin
//
// n1..n3 are workers; "lock" and "printer" are services. The printer cannot
// check fencing tokens, so the only thing keeping two workers from printing at
// once is timing.
//
// This worker trusts the lock's expires_at and its own clock up to the last
// millisecond. That holds only if every clock agrees, every reply is fast and
// no process ever stops.
//
// TODO: measure the lease on this node's own clock (n.Elapsed()), starting
// from the moment you sent acquire (not when the reply arrived), and treat it
// as over margin before lease_ms has passed. Check the time left before every
// page; if a grant arrives with no time left, ask again.
package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"quorumlab"
)

const margin = 50 * time.Millisecond

func main() {
	n := quorumlab.NewNode()

	held := false
	var expiresAt time.Duration // the lock's expires_at for our lease

	acquire := func(ctx context.Context) {
		for {
			res, err := n.Call(ctx, "lock", map[string]any{"type": "acquire"}, time.Second)
			var r struct {
				Granted   bool  `json:"granted"`
				ExpiresAt int64 `json:"expires_at"`
			}
			if err == nil && res.Decode(&r) == nil && r.Granted {
				held = true
				expiresAt = time.Duration(r.ExpiresAt) * time.Millisecond
				return
			}
			time.Sleep(time.Duration(15+rand.Intn(20)) * time.Millisecond)
		}
	}

	n.Handle("job", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Job   string `json:"job"`
			Pages int    `json:"pages"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		for page := 1; page <= req.Pages; page++ {
			if !held || n.Elapsed() >= expiresAt {
				acquire(ctx)
			}
			_, _ = n.Call(ctx, "printer", map[string]any{"type": "print", "job": req.Job, "page": page}, time.Second)
		}
		held = false
		_ = n.Send(ctx, "lock", map[string]any{"type": "release"})
		return n.Reply(msg, map[string]any{"type": "job_ok", "done": true})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
