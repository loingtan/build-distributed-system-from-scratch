// Fencing tokens. Read the brief: npm run lab -- brief lease-fencing
//
// n1 is the lock server, n2 the storage, n3..n5 the workers.
//
// This works while every worker stays awake: n1 hands out leases, a worker
// takes the lock, reads the list from n2, appends its job and writes it back.
// But a worker that is paused after its read (or whose write is slow) wakes up
// after its lease ended and writes back an old list, wiping out jobs another
// worker finished in the meantime.
//
// TODO, n1: give every new grant a token, higher than any before it, in
// acquire_ok ({ granted: true, lease_ms, token }). The same holder acquiring
// again keeps its token.
// TODO, n2: every read and write carries a token. Refuse ({ type: "rejected" })
// any token lower than the highest one seen so far, on reads as well as writes.
// TODO, workers: send your token with every read and write.
package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"quorumlab"
)

const (
	leaseMs = 300
	lockID  = "n1"
	storeID = "n2"
)

func main() {
	n := quorumlab.NewNode()

	// ---- lock server (n1)

	var lockMu sync.Mutex
	holder := ""
	var expires time.Time

	heldBy := func(c string) bool { return holder == c && time.Now().Before(expires) }
	isFree := func() bool { return holder == "" || !time.Now().Before(expires) }

	n.Handle("acquire", func(ctx context.Context, msg quorumlab.Message) error {
		lockMu.Lock()
		defer lockMu.Unlock()
		switch {
		case heldBy(msg.Src):
		case isFree():
			holder = msg.Src
		default:
			return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": false, "lease_ms": leaseMs})
		}
		expires = time.Now().Add(leaseMs * time.Millisecond)
		return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": true, "lease_ms": leaseMs})
	})

	n.Handle("renew", func(ctx context.Context, msg quorumlab.Message) error {
		lockMu.Lock()
		defer lockMu.Unlock()
		ok := heldBy(msg.Src)
		if ok {
			expires = time.Now().Add(leaseMs * time.Millisecond)
		}
		return n.Reply(msg, map[string]any{"type": "renew_ok", "ok": ok})
	})

	n.Handle("release", func(ctx context.Context, msg quorumlab.Message) error {
		lockMu.Lock()
		defer lockMu.Unlock()
		if heldBy(msg.Src) {
			holder = ""
		}
		return n.Reply(msg, map[string]any{"type": "release_ok"})
	})

	// ---- storage (n2)

	var storeMu sync.Mutex
	entries := []string{}

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		storeMu.Lock()
		defer storeMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "entries": entries})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Entries []string `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		storeMu.Lock()
		defer storeMu.Unlock()
		entries = append([]string{}, req.Entries...)
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("inspect", func(ctx context.Context, msg quorumlab.Message) error {
		storeMu.Lock()
		defer storeMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "inspect_ok", "entries": entries})
	})

	// ---- workers (n3..n5)

	// acquire asks for the lock until it is granted, for up to 1.5s.
	acquire := func(ctx context.Context) bool {
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			res, err := n.Call(ctx, lockID, map[string]any{"type": "acquire"}, 300*time.Millisecond)
			var r struct {
				Granted bool `json:"granted"`
			}
			if err == nil && res.Decode(&r) == nil && r.Granted {
				return true
			}
			time.Sleep(time.Duration(15+rand.Intn(20)) * time.Millisecond)
		}
		return false
	}

	n.Handle("job", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Job string `json:"job"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if !acquire(ctx) {
			return n.Reply(msg, map[string]any{"type": "job_ok", "done": false})
		}
		done := false
		res, err := n.Call(ctx, storeID, map[string]any{"type": "read"}, time.Second)
		var read struct {
			Entries []string `json:"entries"`
		}
		if err == nil && res.Type() == "read_ok" && res.Decode(&read) == nil {
			next := append(read.Entries, req.Job)
			res, err := n.Call(ctx, storeID, map[string]any{"type": "write", "entries": next}, time.Second)
			done = err == nil && res.Type() == "write_ok"
		}
		_ = n.Send(ctx, lockID, map[string]any{"type": "release"})
		return n.Reply(msg, map[string]any{"type": "job_ok", "done": done})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
