// Fencing tokens. n1 is the lock server, n2 the storage, n3..n5 the workers.
//
// A lease alone cannot stop a holder that was paused (or whose message was
// slow) past its expiry: when it wakes up it still believes it holds the lock.
// So every grant carries a token that is higher than any before it, the worker
// sends that token with every request to the storage, and the storage refuses
// any token lower than the highest it has seen. The stale holder's write is
// rejected however late it arrives.
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
	token := 0 // the token of the current (or last) grant

	heldBy := func(c string) bool { return holder == c && time.Now().Before(expires) }
	isFree := func() bool { return holder == "" || !time.Now().Before(expires) }

	n.Handle("acquire", func(ctx context.Context, msg quorumlab.Message) error {
		lockMu.Lock()
		defer lockMu.Unlock()
		switch {
		case heldBy(msg.Src):
			// same holder: same token, fresh lease
		case isFree():
			holder = msg.Src
			token++ // every new grant gets a higher token
		default:
			return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": false, "lease_ms": leaseMs})
		}
		expires = time.Now().Add(leaseMs * time.Millisecond)
		return n.Reply(msg, map[string]any{"type": "acquire_ok", "granted": true, "lease_ms": leaseMs, "token": token})
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
	highest := 0 // the highest token any request has carried

	type storeReq struct {
		Token   *int     `json:"token"`
		Entries []string `json:"entries"`
	}
	// fenced reports whether the request carries a token lower than the highest
	// seen. Reads count too: once the new holder has read, the old one must not
	// write. Call with storeMu held.
	fenced := func(req storeReq) bool {
		if req.Token == nil || *req.Token < highest {
			return true
		}
		highest = *req.Token
		return false
	}

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req storeReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		storeMu.Lock()
		defer storeMu.Unlock()
		if fenced(req) {
			return n.Reply(msg, map[string]any{"type": "rejected", "highest": highest})
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "entries": entries})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req storeReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		storeMu.Lock()
		defer storeMu.Unlock()
		if fenced(req) {
			return n.Reply(msg, map[string]any{"type": "rejected", "highest": highest})
		}
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
	acquire := func(ctx context.Context) (int, bool) {
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			res, err := n.Call(ctx, lockID, map[string]any{"type": "acquire"}, 300*time.Millisecond)
			var r struct {
				Granted bool `json:"granted"`
				Token   int  `json:"token"`
			}
			if err == nil && res.Decode(&r) == nil && r.Granted {
				return r.Token, true
			}
			time.Sleep(time.Duration(15+rand.Intn(20)) * time.Millisecond)
		}
		return 0, false
	}

	n.Handle("job", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Job string `json:"job"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		t, ok := acquire(ctx)
		if !ok {
			return n.Reply(msg, map[string]any{"type": "job_ok", "done": false})
		}
		done := false
		res, err := n.Call(ctx, storeID, map[string]any{"type": "read", "token": t}, time.Second)
		var read struct {
			Entries []string `json:"entries"`
		}
		if err == nil && res.Type() == "read_ok" && res.Decode(&read) == nil {
			next := append(read.Entries, req.Job)
			res, err := n.Call(ctx, storeID, map[string]any{"type": "write", "token": t, "entries": next}, time.Second)
			done = err == nil && res.Type() == "write_ok"
		}
		_ = n.Send(ctx, lockID, map[string]any{"type": "release"})
		return n.Reply(msg, map[string]any{"type": "job_ok", "done": done})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
