// Global secondary index: records are partitioned by key, the index by term
// (color), and index updates are sent asynchronously, ordered by version.
package main

import (
	"context"
	"encoding/json"
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

// record is the stored value (kept as raw JSON so it comes back unchanged)
// with its color and the version the owner gave it.
type record struct {
	Value   json.RawMessage
	Color   string
	Version int
}

// indexUpdate says: in the list for Color, Key is (Present) or is not
// (!Present) there as of record version Version.
type indexUpdate struct {
	Color   string `json:"color"`
	Key     string `json:"key"`
	Version int    `json:"version"`
	Present bool   `json:"present"`
}

type entry struct {
	Version int
	Present bool
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	records := map[string]record{}         // the keys this node owns
	index := map[string]map[string]entry{} // color -> key -> entry, for the colors this node indexes

	// Records are partitioned by key. The index is partitioned by term: the
	// list of keys for one color lives on one node, wherever the records are.
	pick := func(s string) string {
		ids := n.NodeIDs()
		return ids[hash(s)%uint32(len(ids))]
	}
	ownerOf := func(key string) string { return pick(key) }
	indexNodeOf := func(color string) string { return pick("color:" + color) }

	// Index updates travel on their own and may overtake each other. Each
	// carries the version of the record that caused it, and an entry changes
	// only for a newer version, so the outcome does not depend on arrival
	// order. A removal is kept as Present: false so that an older add
	// arriving later cannot bring the key back.
	applyIndex := func(u indexUpdate) {
		mu.Lock()
		defer mu.Unlock()
		if index[u.Color] == nil {
			index[u.Color] = map[string]entry{}
		}
		if cur, ok := index[u.Color][u.Key]; !ok || u.Version > cur.Version {
			index[u.Color][u.Key] = entry{u.Version, u.Present}
		}
	}
	sendIndex := func(ctx context.Context, u indexUpdate) {
		dest := indexNodeOf(u.Color)
		if dest == n.ID() {
			applyIndex(u)
			return
		}
		n.Send(ctx, dest, map[string]any{"type": "index", "color": u.Color, "key": u.Key, "version": u.Version, "present": u.Present})
	}

	n.Handle("index", func(ctx context.Context, msg quorumlab.Message) error {
		var u indexUpdate
		if err := msg.Decode(&u); err != nil {
			return err
		}
		applyIndex(u)
		return nil
	})

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if owner := ownerOf(req.Key); owner != n.ID() {
			if _, err := n.Call(ctx, owner, map[string]any{"type": "put", "key": req.Key, "value": req.Value}, time.Second); err != nil {
				return nil
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		var v struct {
			Color string `json:"color"`
		}
		if err := json.Unmarshal(req.Value, &v); err != nil {
			return err
		}
		mu.Lock()
		old, had := records[req.Key]
		version := old.Version + 1
		records[req.Key] = record{req.Value, v.Color, version}
		mu.Unlock()
		// Asynchronous index maintenance: reply now, the index catches up shortly.
		if had && old.Color != v.Color {
			sendIndex(ctx, indexUpdate{old.Color, req.Key, version, false})
		}
		sendIndex(ctx, indexUpdate{v.Color, req.Key, version, true})
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if owner := ownerOf(req.Key); owner != n.ID() {
			res, err := n.Call(ctx, owner, map[string]any{"type": "get", "key": req.Key}, time.Second)
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
		}
		mu.Lock()
		r, ok := records[req.Key]
		mu.Unlock()
		value := json.RawMessage("null")
		if ok {
			value = r.Value
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	lookup := func(color string) []string {
		mu.Lock()
		defer mu.Unlock()
		keys := []string{}
		for k, e := range index[color] {
			if e.Present {
				keys = append(keys, k)
			}
		}
		return keys
	}

	type colorReq struct {
		Color string `json:"color"`
	}

	n.Handle("index_lookup", func(ctx context.Context, msg quorumlab.Message) error {
		var req colorReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "index_lookup_ok", "keys": lookup(req.Color)})
	})

	// One node holds the whole answer: no scatter, no gather.
	n.Handle("find", func(ctx context.Context, msg quorumlab.Message) error {
		var req colorReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		dest := indexNodeOf(req.Color)
		if dest == n.ID() {
			return n.Reply(msg, map[string]any{"type": "find_ok", "keys": lookup(req.Color)})
		}
		res, err := n.Call(ctx, dest, map[string]any{"type": "index_lookup", "color": req.Color}, time.Second)
		if err != nil {
			return nil
		}
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := res.Decode(&body); err != nil {
			return err
		}
		if body.Keys == nil {
			body.Keys = []string{}
		}
		return n.Reply(msg, map[string]any{"type": "find_ok", "keys": body.Keys})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
