// Sticky routing with rendezvous hashing: every request for a key goes to
// the same backend, and a pool change moves only the keys it has to.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const backendTimeout = 1000 * time.Millisecond

// relay passes a backend's answer back to the client unchanged (only the
// backend's own message id is dropped; Reply adds in_reply_to).
func relay(n *quorumlab.Node, req, res quorumlab.Message) error {
	var body map[string]any
	if err := res.Decode(&body); err != nil {
		return err
	}
	delete(body, "msg_id")
	delete(body, "in_reply_to")
	return n.Reply(req, body)
}

// hash is a deterministic string hash (FNV-1a with a final mix). The same
// string gives the same 32-bit number on every node, every time.
func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, c := range []rune(s) {
		h ^= uint32(c)
		h *= 16777619
	}
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// pick uses rendezvous (highest random weight) hashing: every backend gets a
// score for this key, and the highest score wins. The score depends only on
// the key and the backend's id, so the order of the list does not matter, and
// a change to the pool only affects the keys whose winner left or whose new
// winner joined.
func pick(backends []string, key string) string {
	best := ""
	var bestScore int64 = -1
	for _, id := range backends {
		score := int64(hash(key + "/" + id))
		if score > bestScore || (score == bestScore && id < best) {
			best, bestScore = id, score
		}
	}
	return best
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var backends []string // the pool, from the latest set_backends

	n.Handle("set_backends", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Backends []string `json:"backends"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		backends = req.Backends
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_backends_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		pool := backends
		mu.Unlock()
		if len(pool) == 0 {
			return n.Reply(msg, map[string]any{"type": "error", "code": "no_backends"})
		}
		backend := pick(pool, req.Key)
		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		return relay(n, msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
