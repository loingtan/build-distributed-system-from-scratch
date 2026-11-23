// B-tree pages. Read the brief: npm run lab -- brief store-btree
package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sync"

	"quorumlab"
)

const maxKeys = 4 // per page

// page is one node of the tree. An inner page with keys [k0, k1] has three
// children: keys < k0, keys in [k0, k1), and keys >= k1. Values live only in
// the leaves.
type page struct {
	Leaf     bool              `json:"leaf"`
	Keys     []string          `json:"keys"`
	Values   []json.RawMessage `json:"values,omitempty"`
	Children []string          `json:"children,omitempty"`
}

// MarshalJSON always writes "values" for a leaf, even an empty one.
func (p page) MarshalJSON() ([]byte, error) {
	keys := p.Keys
	if keys == nil {
		keys = []string{}
	}
	if p.Leaf {
		values := p.Values
		if values == nil {
			values = []json.RawMessage{}
		}
		return json.Marshal(map[string]any{"leaf": true, "keys": keys, "values": values})
	}
	return json.Marshal(map[string]any{"leaf": false, "keys": keys, "children": p.Children})
}

// tree is the on-disk state: "pages", "root" and "next" (the next unused id).
type tree struct {
	pages map[string]page
	root  string
	next  int
}

// childIndex is the child to follow for key: the number of keys <= key.
func childIndex(p page, key string) int {
	i := 0
	for i < len(p.Keys) && p.Keys[i] <= key {
		i++
	}
	return i
}

// findLeaf returns the id of the leaf that key belongs in.
func (t *tree) findLeaf(key string) string {
	id := t.root
	for !t.pages[id].Leaf {
		p := t.pages[id]
		id = p.Children[childIndex(p, key)]
	}
	return id
}

// put: for now every key goes into the one leaf it belongs to, which never
// splits.
// TODO: when a page would hold more than maxKeys keys, split it in two and
// add the separator key to its parent (which may split in turn). A full root
// becomes two pages under a new root. Also make every write crash-safe: write
// new copies of the changed pages and switch root to the new root in one step
// (copy-on-write), or log the page changes first.
func (t *tree) put(key string, value json.RawMessage) {
	id := t.findLeaf(key)
	p := t.pages[id]
	if i := slices.Index(p.Keys, key); i >= 0 {
		p.Values[i] = value
	} else {
		at := childIndex(p, key)
		p.Keys = slices.Insert(p.Keys, at, key)
		p.Values = slices.Insert(p.Values, at, value)
	}
	t.pages[id] = p
}

func (t *tree) remove(key string) {
	id := t.findLeaf(key)
	p := t.pages[id]
	if i := slices.Index(p.Keys, key); i >= 0 {
		p.Keys = slices.Delete(p.Keys, i, i+1)
		p.Values = slices.Delete(p.Values, i, i+1)
	}
	t.pages[id] = p
}

func (t *tree) lookup(key string) (json.RawMessage, bool) {
	p := t.pages[t.root]
	for !p.Leaf {
		p = t.pages[p.Children[childIndex(p, key)]]
	}
	if i := slices.Index(p.Keys, key); i >= 0 {
		return p.Values[i], true
	}
	return nil, false
}

// scan visits every subtree that can hold keys in [from, to), in key order.
func (t *tree) scan(id, from, to string, out *[][2]any) {
	p := t.pages[id]
	if p.Leaf {
		for i, k := range p.Keys {
			if k >= from && k < to {
				*out = append(*out, [2]any{k, p.Values[i]})
			}
		}
		return
	}
	for i, c := range p.Children {
		lowOK := i == 0 || p.Keys[i-1] < to
		highOK := i == len(p.Keys) || p.Keys[i] > from
		if lowOK && highOK {
			t.scan(c, from, to, out)
		}
	}
}

func main() {
	n := quorumlab.NewNode()

	// mu makes every request one step.
	var mu sync.Mutex
	t := &tree{}

	// save writes the whole tree in one fsynced disk update.
	save := func() error {
		return n.Disk().Update(func(tx *quorumlab.Tx) error {
			if err := tx.Set("pages", t.pages); err != nil {
				return err
			}
			if err := tx.Set("root", t.root); err != nil {
				return err
			}
			return tx.Set("next", t.next)
		})
	}

	n.OnStart(func(ctx context.Context) {
		ok, err := n.Disk().Get("pages", &t.pages)
		if err != nil {
			log.Fatalf("reading the pages: %v", err)
		}
		if !ok {
			t.pages = map[string]page{"1": {Leaf: true, Keys: []string{}, Values: []json.RawMessage{}}}
			t.root, t.next = "1", 2
			if err := save(); err != nil {
				log.Fatalf("creating the tree: %v", err)
			}
			return
		}
		if _, err := n.Disk().Get("root", &t.root); err != nil {
			log.Fatalf("reading the root: %v", err)
		}
		if _, err := n.Disk().Get("next", &t.next); err != nil {
			log.Fatalf("reading next: %v", err)
		}
	})

	type request struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
		From  string          `json:"from"`
		To    string          `json:"to"`
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		t.put(req.Key, req.Value)
		if err := save(); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("delete", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		t.remove(req.Key)
		if err := save(); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "delete_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		v, ok := t.lookup(req.Key)
		mu.Unlock()
		if !ok {
			v = json.RawMessage("null")
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
	})

	n.Handle("scan", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		entries := [][2]any{}
		mu.Lock()
		t.scan(t.root, req.From, req.To, &entries)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "scan_ok", "entries": entries})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
