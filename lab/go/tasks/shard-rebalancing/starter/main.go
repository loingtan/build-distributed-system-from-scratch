// Rebalancing. Read the brief: npm run lab -- brief shard-rebalancing
package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strconv"
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

type point struct {
	pos uint32
	id  string
}

// points is the number of virtual nodes per member.
const points = 200

// buildRing puts many points per member. Each member's share of the ring is
// the sum of many small arcs, so the shares even out.
func buildRing(members []string) []point {
	ring := make([]point, 0, len(members)*points)
	for _, id := range members {
		for i := 0; i < points; i++ {
			ring = append(ring, point{hash(id + "#" + strconv.Itoa(i)), id})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		if ring[i].pos != ring[j].pos {
			return ring[i].pos < ring[j].pos
		}
		return ring[i].id < ring[j].id
	})
	return ring
}

// ownerOf: the owner of a key is the node at the first point at or after the
// key's hash, going clockwise; past the last point, wrap round to the first.
// A binary search keeps lookups fast on a ring of over a thousand points.
func ownerOf(ring []point, key string) string {
	h := hash(key)
	i := sort.Search(len(ring), func(i int) bool { return ring[i].pos >= h })
	if i == len(ring) {
		i = 0
	}
	return ring[i].id
}

type request struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]json.RawMessage{} // the keys stored on this node
	var ring []point                      // sorted by pos

	owner := func(key string) string {
		mu.Lock()
		defer mu.Unlock()
		return ownerOf(ring, key)
	}

	// Build the ring once at startup too, from every node.
	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		ring = buildRing(n.NodeIDs())
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
		ring = buildRing(req.Members)
		mu.Unlock()
		// After this, some keys in the store belong to another node. Nothing moves
		// them, so a get routed to the new owner finds nothing.
		// TODO: hand those keys to their new owners.
		//   - Send each new owner its keys and values, and wait for its answer.
		//   - Delete a key here only once the new owner has confirmed it.
		//   - If no answer comes, try again later (n.Every in n.OnStart).
		//   - The receiver must not replace a value it already has: a put that
		//     reached it directly is newer than anything being handed over.
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
			owners[i] = ownerOf(ring, k)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "lookup_ok", "owners": owners})
	})

	// put and get take the same path: do the work here if this node owns the
	// key, otherwise pass the request to the owner and relay its answer.
	// TODO: one test loses messages between nodes. A forwarded request that gets
	// no answer must be sent again, or the client never hears back.
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
