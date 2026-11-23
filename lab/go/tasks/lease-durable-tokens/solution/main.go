// Tokens that survive a crash. n1 is the lock server, n2 the storage, n3..n5
// the workers.
//
// Fencing only works if tokens never go backwards. So n1 keeps its counter on
// disk, and n2 keeps its list and the highest token it has seen there: a
// crash loses memory, not promises already made.
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
	// The token counter lives on disk: after a crash, n1 must carry on from
	// the last token it handed out, never below it. (The holder and expiry may
	// be forgotten: a stale holder's requests carry an old token and n2
	// refuses them.)
	token := 0

	heldBy := func(c string) bool { return holder == c && time.Now().Before(expires) }
	isFree := func() bool { return holder == "" || !time.Now().Before(expires) }

	n.Handle("acquire", func(ctx context.Context, msg quorumlab.Message) error {
		lockMu.Lock()
		defer lockMu.Unlock()
		switch {
		case heldBy(msg.Src):
			// same holder: same token, fresh lease
		case isFree():
			// On disk before the reply that hands it out.
			if err := n.Disk().Set("token", token+1); err != nil {
				return err
			}
			holder = msg.Src
			token++
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
	// The list and the highest token survive a crash. Each is on disk before
	// the reply that depends on it.
	entries := []string{}
	highest := 0

	type storeReq struct {
		Token   *int     `json:"token"`
		Entries []string `json:"entries"`
	}
	// fenced reports whether the request carries a token lower than the highest
	// seen. Reads count too: once the new holder has read, the old one must not
	// write. Call with storeMu held.
	fenced := func(req storeReq) (bool, error) {
		if req.Token == nil || *req.Token < highest {
			return true, nil
		}
		if *req.Token > highest {
			if err := n.Disk().Set("highest", *req.Token); err != nil {
				return false, err
			}
			highest = *req.Token
		}
		return false, nil
	}

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req storeReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		storeMu.Lock()
		defer storeMu.Unlock()
		if stale, err := fenced(req); err != nil {
			return err
		} else if stale {
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
		if stale, err := fenced(req); err != nil {
			return err
		} else if stale {
			return n.Reply(msg, map[string]any{"type": "rejected", "highest": highest})
		}
		next := append([]string{}, req.Entries...)
		if err := n.Disk().Set("entries", next); err != nil {
			return err
		}
		entries = next
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("inspect", func(ctx context.Context, msg quorumlab.Message) error {
		storeMu.Lock()
		defer storeMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "inspect_ok", "entries": entries})
	})

	// After a restart, pick up where the disk says we were.
	n.OnStart(func(ctx context.Context) {
		lockMu.Lock()
		_, _ = n.Disk().Get("token", &token)
		lockMu.Unlock()
		storeMu.Lock()
		_, _ = n.Disk().Get("entries", &entries)
		_, _ = n.Disk().Get("highest", &highest)
		storeMu.Unlock()
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
