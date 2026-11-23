// Consistent hashing. Read the brief: npm run lab -- brief shard-consistent-hashing
package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
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

// ownerOf is the previous task's rule, applied to the member list. It routes
// correctly, but when the list changes length almost every key gets a new owner.
// TODO: replace it with a hash ring.
//   - Give each member a position: hash(id).
//   - Keep the positions sorted.
//   - A key belongs to the first member at or after hash(key), wrapping
//     round to the smallest position after the largest.
func ownerOf(members []string, key string) string {
	return members[hash(key)%uint32(len(members))]
}

type request struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]json.RawMessage{} // the keys this node owns
	var members []string                  // the nodes that currently own keys

	owner := func(key string) string {
		mu.Lock()
		defer mu.Unlock()
		return ownerOf(members, key)
	}

	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		members = n.NodeIDs()
		mu.Unlock()
	})

	n.Handle("set_members", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Members []string `json:"members"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		members = append([]string(nil), req.Members...)
		sort.Strings(members)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_members_ok"})
	})

	n.Handle("lookup", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Keys []string `json:"keys"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		owners := make([]string, len(req.Keys))
		for i, k := range req.Keys {
			owners[i] = ownerOf(members, k)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "lookup_ok", "owners": owners})
	})

	// put and get take the same path: do the work here if this node owns the
	// key, otherwise pass the request to the owner and relay its answer.
	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		o := owner(req.Key)
		if o == n.ID() {
			mu.Lock()
			store[req.Key] = req.Value
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		if _, err := n.Call(ctx, o, map[string]any{"type": "put", "key": req.Key, "value": req.Value}, time.Second); err != nil {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		o := owner(req.Key)
		if o == n.ID() {
			mu.Lock()
			v, ok := store[req.Key]
			mu.Unlock()
			if !ok {
				v = json.RawMessage("null")
			}
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
		}
		res, err := n.Call(ctx, o, map[string]any{"type": "get", "key": req.Key}, time.Second)
		if err != nil {
			return nil
		}
		var body struct {
			Value json.RawMessage `json:"value"`
		}
		if err := res.Decode(&body); err != nil {
			return err
		}
		if body.Value == nil {
			body.Value = json.RawMessage("null")
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

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
