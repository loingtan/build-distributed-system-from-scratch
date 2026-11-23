// Global secondary index. Read the brief: npm run lab -- brief shard-secondary-index
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

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	records := map[string]record{} // the keys this node owns

	ownerOf := func(key string) string {
		ids := n.NodeIDs()
		return ids[hash(key)%uint32(len(ids))]
	}

	// TODO: a global index, partitioned by term. Pick one node per color, for
	// example ids[hash("color:"+color) % N], to hold the list of keys with that
	// color. When a put changes a record, tell the index node of the old color
	// and of the new one. Those updates can overtake each other on the way:
	// use the record's version so an older update never undoes a newer one.

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
		old := records[req.Key]
		records[req.Key] = record{req.Value, v.Color, old.Version + 1}
		mu.Unlock()
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

	// localMatches is the keys of this node's own records that have the
	// color: a local index.
	localMatches := func(color string) []string {
		mu.Lock()
		defer mu.Unlock()
		keys := []string{}
		for k, r := range records {
			if r.Color == color {
				keys = append(keys, k)
			}
		}
		return keys
	}

	type colorReq struct {
		Color string `json:"color"`
	}

	n.Handle("find_local", func(ctx context.Context, msg quorumlab.Message) error {
		var req colorReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "find_local_ok", "keys": localMatches(req.Color)})
	})

	// For now a find scatters to every node and gathers the answers. Always
	// correct, and every query costs a message to every node.
	n.Handle("find", func(ctx context.Context, msg quorumlab.Message) error {
		var req colorReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		var wg sync.WaitGroup
		var fmu sync.Mutex
		keys := []string{}
		failed := false
		for _, id := range n.NodeIDs() {
			if id == n.ID() {
				mine := localMatches(req.Color)
				fmu.Lock()
				keys = append(keys, mine...)
				fmu.Unlock()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := n.Call(ctx, id, map[string]any{"type": "find_local", "color": req.Color}, time.Second)
				var body struct {
					Keys []string `json:"keys"`
				}
				if err == nil {
					err = res.Decode(&body)
				}
				fmu.Lock()
				defer fmu.Unlock()
				if err != nil {
					failed = true
					return
				}
				keys = append(keys, body.Keys...)
			}()
		}
		wg.Wait()
		if failed {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "find_ok", "keys": keys})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
