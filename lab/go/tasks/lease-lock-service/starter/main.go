// A lock with a lease. Read the brief: npm run lab -- brief lease-lock-service
//
// n1 is the lock server; the clients are c1, c2, c3.
//
// This server hands out a lock and takes it back on release. It has no
// leases: a client that goes silent while holding the lock keeps it forever.
//
// TODO: give every grant a lease of 300ms (leaseMs). The lock is free again
// once the lease has run out. "renew" extends the lease to 300ms from now, but
// only for the current holder of a lease that has not expired; anyone else
// gets ok: false. "release" from anyone but the current holder is ignored.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

const leaseMs = 300

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	holder := "" // client id, or "" for nobody

	n.Handle("acquire", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		granted := holder == "" || holder == msg.Src
		if granted {
			holder = msg.Src
		}
		return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": granted, "lease_ms": leaseMs})
	})

	n.Handle("renew", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "renew_ok", "ok": holder == msg.Src})
	})

	n.Handle("release", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if holder == msg.Src {
			holder = ""
		}
		return n.Reply(msg, map[string]any{"type": "release_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
