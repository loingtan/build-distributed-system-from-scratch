// Hot spots. Read the brief: npm run lab -- brief shard-hot-spot
package main

import (
	"context"
	"log"
	"sync"
	"time"
	"unicode/utf16"

	"quorumlab"
)

// hash is a deterministic string hash (FNV-1a with a final mix). The same
// string gives the same 32-bit number (0 .. 4294967295) on every node, every
// time. It works on UTF-16 code units, exactly like the browser version.
func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, c := range utf16.Encode([]rune(s)) {
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

type keyReq struct {
	Key   string `json:"key"`
	Delta int    `json:"delta"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	counters := map[string]int{} // key -> count, for the keys this node owns

	ownerOf := func(key string) string {
		ids := n.NodeIDs()
		return ids[hash(key)%uint32(len(ids))]
	}

	// Every request for a key ends up on its one owner. That is fine for a
	// hundred quiet keys, and a disaster when one of them gets most of the
	// traffic.
	// TODO: count requests per key on the owner; once a key is hot, tell
	// every node, spread its increments over several nodes (salting) and have
	// reads of it add up all the parts. Keep cold keys on their owner.
	n.Handle("incr", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if owner := ownerOf(req.Key); owner != n.ID() {
			if _, err := n.Call(ctx, owner, map[string]any{"type": "incr", "key": req.Key, "delta": req.Delta}, time.Second); err != nil {
				return nil
			}
			return n.Reply(msg, map[string]any{"type": "incr_ok"})
		}
		mu.Lock()
		counters[req.Key] += req.Delta
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "incr_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if owner := ownerOf(req.Key); owner != n.ID() {
			res, err := n.Call(ctx, owner, map[string]any{"type": "get", "key": req.Key}, time.Second)
			if err != nil {
				return nil
			}
			var body struct {
				Value int `json:"value"`
			}
			if err := res.Decode(&body); err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
		}
		mu.Lock()
		v := counters[req.Key]
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
