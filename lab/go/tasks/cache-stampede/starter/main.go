// Cache stampede. Read the brief: npm run lab -- brief cache-stampede
//
// The LRU cache with expiry from the last task. When a popular key is
// missing (cold start, or its entry just expired), every get that arrives
// during the database read is a miss too, and each sends its own read.
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
	dbTimeout = 300 * time.Millisecond  // to wait for the database before giving up
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
	// TODO: inflight, a map from key to the read in progress (a struct with a
	// done channel that waiters block on, and the result, set before done is
	// closed).

	// read asks the database for key and fills the cache.
	read := func(ctx context.Context, key string) (any, bool) {
		sentAt := n.Elapsed()
		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		if err != nil || res.Type() != "read_ok" || res.Decode(&body) != nil {
			return nil, false
		}
		mu.Lock()
		cache.store(key, body.Value, sentAt)
		mu.Unlock()
		return body.Value, true
	}

	// load returns the value of key from the cache or the database.
	// TODO: if a read for this key is already in flight, wait for it and share
	// its result instead of starting another. Remove the key from inflight when
	// the read ends, however it ends.
	load := func(ctx context.Context, key string) (any, bool) {
		mu.Lock()
		if v, ok := cache.lookup(key, n.Elapsed()); ok {
			mu.Unlock()
			return v, true
		}
		mu.Unlock()
		return read(ctx, key)
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		value, ok := load(ctx, req.Key)
		if !ok {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
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
