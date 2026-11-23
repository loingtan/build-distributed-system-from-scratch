// Catching up. Read the brief: npm run lab -- brief repl-catch-up
//
// This is the asynchronous solution from the first task. It sends each write
// to each backup exactly once and keeps everything in memory.
//
// What is missing:
//   - a backup that missed messages (lost, partitioned, crashed) never gets
//     them again. With a per-key seq it cannot even tell that it missed some.
//     Keep an ordered log instead: the position in the log is the sequence
//     number, and "I have 17 entries" says exactly what to send next.
//   - nothing survives a crash. n.Disk() does. Keep the log under the disk
//     key "log": n.Disk().Set("log", entries) before you reply, and read it
//     back in n.OnStart with n.Disk().Get("log", &entries).
//   - a restarted primary starts counting from 0 again, so backups ignore
//     its new writes as old ones.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// version is a value and the position of its write in the primary's order.
type version struct {
	value any
	seq   int
}

func main() {
	n := quorumlab.NewNode()

	// Node ids are known once the node has started.
	var primary string
	var isPrimary bool
	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary
	})

	var mu sync.Mutex
	store := map[string]version{} // key -> {value, seq}
	seq := 0

	// The caller holds mu.
	apply := func(key string, value any, s int) {
		if current, ok := store[key]; !ok || s > current.seq {
			store[key] = version{value, s}
		}
	}

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}

		if !isPrimary {
			res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 500*time.Millisecond)
			if err != nil {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			return n.Reply(msg, map[string]any{"type": res.Type()})
		}

		mu.Lock()
		seq++
		s := seq
		apply(req.Key, req.Value, s)
		mu.Unlock()
		if err := n.Reply(msg, map[string]any{"type": "write_ok"}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "key": req.Key, "value": req.Value, "seq": s})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
			Seq   int    `json:"seq"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		apply(req.Key, req.Value, req.Seq)
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key].value // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
