// Hash partitioning. Read the brief: npm run lab -- brief shard-hash-routing
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
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

// TODO: decide which node owns a key. Every node must reach the same answer
// on its own, so it can only depend on the key and on n.NodeIDs().
func ownerOf(n *quorumlab.Node, key string) string {
	return n.ID()
}

type request struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]json.RawMessage{} // the keys stored on this node

	// For now every node keeps whatever it is handed, so a value written through
	// n1 cannot be read through n2.
	// TODO: if ownerOf(n, key) is another node, forward the request to it with
	// n.Call(ctx, owner, map[string]any{"type": "put", "key": key, "value": value}, time.Second)
	// and relay the answer.
	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		v, ok := store[req.Key]
		mu.Unlock()
		if !ok {
			v = json.RawMessage("null")
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
	})

	// The tests use this to see where each key really lives.
	n.Handle("local_keys", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		keys := make([]string, 0, len(store))
		for k := range store {
			keys = append(keys, k)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_keys_ok", "keys": keys})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
