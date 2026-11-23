// An LRU cache with a time to live: an entry may be served for ttl after
// its read was sent, and is a miss after that.
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
	capacity  = 20
	ttl       = 1000 * time.Millisecond // an entry may be served this long after it was read
	dbTimeout = 1000 * time.Millisecond
)

type entry struct {
	key   string
	value any
	at    time.Duration // when the read was sent (n.Elapsed())
}

// lru keeps its keys in a list, least recently used at the front, plus a map
// from key to list element.
type lru struct {
	order *list.List
	items map[string]*list.Element
}

func newLRU() *lru { return &lru{order: list.New(), items: map[string]*list.Element{}} }

func (c *lru) lookup(key string, now time.Duration) (any, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if now-e.at >= ttl {
		// Expired: the database may have changed since. Treat it as a miss.
		c.order.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.order.MoveToBack(el)
	return e.value, true
}

func (c *lru) store(key string, value any, at time.Duration) {
	if el, ok := c.items[key]; ok {
		c.order.Remove(el)
	}
	c.items[key] = c.order.PushBack(&entry{key, value, at})
	for c.order.Len() > capacity {
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
		value, ok := cache.lookup(req.Key, n.Elapsed())
		mu.Unlock()
		if ok {
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
		}

		// Date the entry from when the read was sent: the value is at least that
		// fresh, and the time the reply spent travelling counts against the TTL.
		sentAt := n.Elapsed()
		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": req.Key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		if err != nil || res.Type() != "read_ok" || res.Decode(&body) != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		mu.Lock()
		cache.store(req.Key, body.Value, sentAt)
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
