// Cache invalidation across nodes: a put drops the key on every node, waits
// for each to confirm, and a generation per key keeps reads that started
// before the write from putting the old value back.
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

func (c *lru) remove(key string) {
	if el, ok := c.items[key]; ok {
		c.order.Remove(el)
		delete(c.items, key)
	}
}

func (c *lru) size() int { return c.order.Len() }

// flight is one database read in progress. Waiters block on done; value and
// ok are set before done is closed. gen is the key's generation when the read
// started.
type flight struct {
	gen   int
	done  chan struct{}
	value any
	ok    bool
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	cache := newLRU()
	inflight := map[string]*flight{} // key -> the read in progress
	gen := map[string]int{}          // key -> generation, bumped by every invalidation

	// invalidate forgets everything this node knows about key: the entry, the
	// read in flight (later misses must start a fresh read), and, through the
	// new generation, the right of any older read to fill the cache when it
	// returns. Call with mu held.
	invalidate := func(key string) {
		gen[key]++
		cache.remove(key)
		delete(inflight, key)
	}

	// load returns the value of key from the cache or the database, with at
	// most one read per key in flight.
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
		f := &flight{gen: gen[key], done: make(chan struct{})}
		inflight[key] = f
		mu.Unlock()

		sentAt := n.Elapsed()
		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		f.ok = err == nil && res.Type() == "read_ok" && res.Decode(&body) == nil
		f.value = body.Value

		mu.Lock()
		if inflight[key] == f {
			delete(inflight, key)
		}
		// Store only if no invalidation happened while the read was out: the
		// value may have been read before the write it would now hide.
		if f.ok && gen[key] == f.gen {
			cache.store(key, f.value, sentAt)
		}
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

	// tell asks one peer to drop key, until it confirms. Messages can be lost.
	tell := func(ctx context.Context, peer, key string) {
		for attempt := 0; attempt < 20; attempt++ {
			if _, err := n.Call(ctx, peer, map[string]any{"type": "invalidate", "key": key}, 100*time.Millisecond); err == nil {
				return
			}
		}
	}

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
		// Delete rather than store the new value: two puts racing through two
		// nodes could otherwise leave the older value cached.
		mu.Lock()
		invalidate(req.Key)
		mu.Unlock()
		var wg sync.WaitGroup
		for _, peer := range n.Peers() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tell(ctx, peer, req.Key)
			}()
		}
		wg.Wait()
		// Only now can no node serve the old value: acknowledge.
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("invalidate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		invalidate(req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "invalidate_ok"})
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
