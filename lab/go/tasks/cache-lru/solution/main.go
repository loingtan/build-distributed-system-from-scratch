// An LRU cache: at most capacity entries; when full, evict the entry that
// was used least recently.
package main

import (
	"container/list"
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	capacity  = 20 // most entries a node may hold
	dbTimeout = 1000 * time.Millisecond
)

type entry struct {
	key   string
	value any
}

// lru keeps its keys in a list, least recently used at the front and most
// recently used at the back, plus a map from key to list element.
type lru struct {
	order *list.List
	items map[string]*list.Element
}

func newLRU() *lru { return &lru{order: list.New(), items: map[string]*list.Element{}} }

func (c *lru) lookup(key string) (any, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	// A hit is a use: move the key to the most recently used end.
	c.order.MoveToBack(el)
	return el.Value.(*entry).value, true
}

func (c *lru) store(key string, value any) {
	if el, ok := c.items[key]; ok {
		el.Value.(*entry).value = value
		c.order.MoveToBack(el)
	} else {
		c.items[key] = c.order.PushBack(&entry{key, value})
	}
	for c.order.Len() > capacity {
		// Evict the least recently used key: the one at the front.
		oldest := c.order.Front()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*entry).key)
	}
}

func (c *lru) size() int { return c.order.Len() }

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	cache := newLRU()

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value, ok := cache.lookup(req.Key)
		mu.Unlock()
		if ok {
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
		}

		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": req.Key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		if err != nil || res.Type() != "read_ok" || res.Decode(&body) != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		mu.Lock()
		cache.store(req.Key, body.Value)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

	n.Handle("stats", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		entries := cache.size()
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "stats_ok", "entries": entries})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
