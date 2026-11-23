// B-tree pages: a balanced tree of small fixed-size pages on disk, with
// copy-on-write updates and an atomic switch of the root.
package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"strconv"
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

// Copy-on-write: a write never changes a page that is in the tree. It writes
// new copies of the pages on the path from the root to the leaf (and the new
// pages a split creates), then switches root to the new root, and frees the
// old copies, all in one disk update. A crash leaves either the old tree or
// the new one, never half of each.
func (t *tree) newPage(p page) string {
	id := strconv.Itoa(t.next)
	t.next++
	t.pages[id] = p
	return id
}

// insert returns the ids that replace page id: one, or two after a split,
// with the separator key between them.
func (t *tree) insert(id, key string, value json.RawMessage, old *[]string) ([]string, string) {
	p := t.pages[id]
	*old = append(*old, id)
	if p.Leaf {
		keys := slices.Clone(p.Keys)
		values := slices.Clone(p.Values)
		if i := slices.Index(keys, key); i >= 0 {
			values[i] = value
		} else {
			at := childIndex(p, key)
			keys = slices.Insert(keys, at, key)
			values = slices.Insert(values, at, value)
		}
		if len(keys) <= maxKeys {
			return []string{t.newPage(page{Leaf: true, Keys: keys, Values: values})}, ""
		}
		mid := (len(keys) + 1) / 2
		left := t.newPage(page{Leaf: true, Keys: keys[:mid:mid], Values: values[:mid:mid]})
		right := t.newPage(page{Leaf: true, Keys: keys[mid:], Values: values[mid:]})
		return []string{left, right}, keys[mid]
	}
	at := childIndex(p, key)
	ids, sep := t.insert(p.Children[at], key, value, old)
	keys := slices.Clone(p.Keys)
	children := slices.Replace(slices.Clone(p.Children), at, at+1, ids...)
	if sep != "" {
		keys = slices.Insert(keys, at, sep)
	}
	if len(keys) <= maxKeys {
		return []string{t.newPage(page{Keys: keys, Children: children})}, ""
	}
	// An inner page splits around its middle key, which moves up to the parent.
	mid := len(keys) / 2
	left := t.newPage(page{Keys: keys[:mid:mid], Children: children[: mid+1 : mid+1]})
	right := t.newPage(page{Keys: keys[mid+1:], Children: children[mid+1:]})
	return []string{left, right}, keys[mid]
}

func (t *tree) remove(id, key string, old *[]string) string {
	p := t.pages[id]
	*old = append(*old, id)
	if p.Leaf {
		keys := slices.Clone(p.Keys)
		values := slices.Clone(p.Values)
		if i := slices.Index(keys, key); i >= 0 {
			keys = slices.Delete(keys, i, i+1)
			values = slices.Delete(values, i, i+1)
		}
		return t.newPage(page{Leaf: true, Keys: keys, Values: values})
	}
	at := childIndex(p, key)
	children := slices.Clone(p.Children)
	children[at] = t.remove(children[at], key, old)
	return t.newPage(page{Keys: slices.Clone(p.Keys), Children: children})
}

// commit makes the new root the tree and frees the old path.
func (t *tree) commit(root string, old []string) {
	t.root = root
	for _, id := range old {
		delete(t.pages, id)
	}
}

func (t *tree) put(key string, value json.RawMessage) {
	var old []string
	ids, sep := t.insert(t.root, key, value, &old)
	// A full root splits into two pages under a new root: the tree grows at
	// the top, so every leaf stays at the same depth.
	root := ids[0]
	if len(ids) == 2 {
		root = t.newPage(page{Keys: []string{sep}, Children: ids})
	}
	t.commit(root, old)
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

	// save writes the whole tree in one atomic, fsynced disk update.
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
		// Deleting a missing key changes nothing. Pages may become under-full
		// or empty; this tree does not merge them.
		if _, ok := t.lookup(req.Key); ok {
			var old []string
			t.commit(t.remove(t.root, req.Key, &old), old)
			if err := save(); err != nil {
				return err
			}
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
