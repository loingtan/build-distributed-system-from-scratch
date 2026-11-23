// Cache invalidation. Read the brief: npm run lab -- brief cache-invalidation
//
// The single-flight cache from the last task, now on three nodes, plus a
// `put` that writes to the database. Nothing tells the caches about the
// write, so every node keeps serving the old value until it expires.
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

// flight is one database read in progress. Waiters block on done; value and
// ok are set before done is closed.
type flight struct {
	done  chan struct{}
	value any
	ok    bool
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	cache := newLRU()
	inflight := map[string]*flight{} // key -> the read in progress

	// TODO: an invalidate(key) that makes this node forget key. Deleting the
	// entry is not enough:
	//   - a read that started before the write may return after the
	//     invalidation and must not put the old value back (keep a per-key
	//     generation number, bump it here, and only store a read whose
	//     generation has not changed);
	//   - a get that arrives after the invalidation must not wait for a read
	//     that started before it.

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

	// load returns the value of key from the cache or the database. Every miss
	// for a key that is already being read waits for that read instead of
	// starting one.
	load := func(ctx context.Context, key string) (any, bool) {
		mu.Lock()
		if v, ok := cache.lookup(key, n.Elapsed()); ok {
			mu.Unlock()
			return v, true
		}
		if f, ok := inflight[key]; ok {
			mu.Unlock()
			<-f.done
			return f.value, f.ok
		}
		f := &flight{done: make(chan struct{})}
		inflight[key] = f
		mu.Unlock()

		f.value, f.ok = read(ctx, key)
		// Forget the read whatever happened, or a failure would block the key forever.
		mu.Lock()
		delete(inflight, key)
		mu.Unlock()
		close(f.done)
		return f.value, f.ok
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

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		res, err := n.Call(ctx, "db", map[string]any{"type": "write", "key": req.Key, "value": req.Value}, dbTimeout)
		if err != nil || res.Type() != "write_ok" {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		// TODO: invalidate the key here, and on every peer. Send each peer
		// {"type": "invalidate", "key": key}, retry until it answers (messages
		// can be lost), and only reply put_ok once every peer has confirmed.
		return n.Reply(msg, map[string]any{"type": "put_ok"})
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
