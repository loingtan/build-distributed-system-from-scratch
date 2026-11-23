// Acting only while the lease lasts. n1..n3 are workers; "lock" and "printer"
// are services. The printer cannot check fencing tokens, so the only thing
// keeping two workers from printing at once is timing.
//
// Two rules make that timing safe:
//  1. Measure the lease on your own clock, starting from the moment you SENT
//     acquire. The lock granted it later than that, so the real lease ends
//     later than your estimate, never earlier, however slow the reply was.
//     Never compare expires_at (the lock's clock) with n.Elapsed() (yours).
//  2. Stop a safety margin before your estimate runs out: a page still has to
//     travel to the printer, and clocks do not all run at exactly one speed.
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

	// By this node's clock: the last moment to send a page. Only the job
	// handler touches it, and a worker gets one job at a time.
	// n.Elapsed() can be negative on a node whose clock runs behind, so "no
	// lease" needs its own flag rather than validUntil == 0.
	held := false
	var validUntil time.Duration
	leaseLeft := func() time.Duration {
		if !held {
			return 0
		}
		return validUntil - n.Elapsed()
	}

	// acquire gets (or extends) the lease. As the holder, acquire extends it.
	// It keeps trying until it has a lease with time left.
	acquire := func(ctx context.Context) {
		for {
			sentAt := n.Elapsed()
			res, err := n.Call(ctx, "lock", map[string]any{"type": "acquire"}, time.Second)
			var r struct {
				Granted bool  `json:"granted"`
				LeaseMs int64 `json:"lease_ms"`
			}
			if err == nil && res.Decode(&r) == nil && r.Granted {
				held = true
				validUntil = sentAt + time.Duration(r.LeaseMs)*time.Millisecond - margin
				// The reply may have been slow, or this process paused as it
				// arrived: the lease can be nearly or entirely gone already.
				// Then ask again.
				if leaseLeft() > 0 {
					return
				}
				continue
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
			// Check right before every page: a page sent after this point
			// could reach the printer after the lease has ended.
			if leaseLeft() <= 0 {
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
