// A lock with a lease. n1 is the lock server; the clients are c1, c2, c3.
//
// The server remembers who holds the lock and until when. A lease that is not
// renewed simply runs out, so a client that crashes or goes silent while
// holding the lock cannot block everyone else forever.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const leaseMs = 300

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	holder := ""          // client id, or "" for nobody
	var expires time.Time // when the holder's lease ends

	// The lock counts as held only while the lease is still running.
	heldBy := func(client string) bool { return holder == client && time.Now().Before(expires) }
	isFree := func() bool { return holder == "" || !time.Now().Before(expires) }
	extend := func() { expires = time.Now().Add(leaseMs * time.Millisecond) }

	n.Handle("acquire", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// A free lock, or one the requester already holds, is granted for a fresh lease.
		granted := isFree() || heldBy(msg.Src)
		if granted {
			holder = msg.Src
			extend()
		}
		return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": granted, "lease_ms": leaseMs})
	})

	n.Handle("renew", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// Only the holder of a lease that has not run out may extend it. After
		// expiry someone else may have held the lock in between: acquire again.
		ok := heldBy(msg.Src)
		if ok {
			extend()
		}
		return n.Reply(msg, map[string]any{"type": "renew_ok", "ok": ok})
	})

	n.Handle("release", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// A release from anyone but the current holder is stale; ignore it.
		if heldBy(msg.Src) {
			holder = ""
		}
		return n.Reply(msg, map[string]any{"type": "release_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
