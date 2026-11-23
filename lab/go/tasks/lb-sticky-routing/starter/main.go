// Sticky routing. Read the brief: npm run lab -- brief lb-sticky-routing
//
// Least outstanding requests from the previous task. It spreads load well,
// but a key lands wherever there is room at that moment. Each backend caches
// the keys it has served, so a key that wanders misses the cache every time.
package main

import (
	"context"
	"log"
	"math"
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

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var backends []string        // the pool, from the latest set_backends
	inFlight := map[string]int{} // backend id -> requests in flight

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

	// TODO: choose the backend from the key alone, so that every request for a
	// key goes to the same backend while the pool is unchanged, and a change to
	// the pool moves only the keys it has to. (Call it with mu held.)
	pick := func(key string) string {
		var best []string
		fewest := math.MaxInt
		for _, id := range backends {
			switch c := inFlight[id]; {
			case c < fewest:
				fewest = c
				best = []string{id}
			case c == fewest:
				best = append(best, id)
			}
		}
		return best[int(n.Rand()*float64(len(best)))]
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		if len(backends) == 0 {
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "error", "code": "no_backends"})
		}
		backend := pick(req.Key)
		inFlight[backend]++
		mu.Unlock()

		res, err := n.Call(ctx, backend, map[string]any{"type": "get", "key": req.Key}, backendTimeout)

		mu.Lock()
		inFlight[backend]--
		mu.Unlock()
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "timeout"})
		}
		return relay(n, msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
