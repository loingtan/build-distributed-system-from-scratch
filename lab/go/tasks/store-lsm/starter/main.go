// Memtable and segments. Read the brief: npm run lab -- brief store-lsm
//
// An LSM tree, half built. Writes go to the memtable and to a log that
// protects it. Nothing is ever flushed, so the log grows without limit and
// there are no segments to read.
package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"strings"
	"sync"

	"quorumlab"
)

const memtableLimit = 8 // writes

// pair is one write, stored as the JSON array [key, value]; a nil Value is a
// tombstone ("this key was deleted").
type pair struct {
	Key   string
	Value *string
}

func (p pair) MarshalJSON() ([]byte, error) { return json.Marshal([2]any{p.Key, p.Value}) }

func (p *pair) UnmarshalJSON(b []byte) error {
	var a [2]json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	if err := json.Unmarshal(a[0], &p.Key); err != nil {
		return err
	}
	return json.Unmarshal(a[1], &p.Value)
}

// segment is { "entries": [[key, value], ...] }, entries sorted by key. It is
// never changed once written.
type segment struct {
	Entries []pair `json:"entries"`
}

// sorted returns the map's entries sorted by key.
func sorted(m map[string]*string) []pair {
	out := make([]pair, 0, len(m))
	for key, value := range m {
		out = append(out, pair{key, value})
	}
	slices.SortFunc(out, func(a, b pair) int { return strings.Compare(a.Key, b.Key) })
	return out
}

func main() {
	n := quorumlab.NewNode()

	// On disk: "log", [key, value] for every write since the last flush, and
	// "segments", oldest first. mu makes every request one step.
	var mu sync.Mutex
	wal := []pair{}
	segments := []segment{}

	// key -> value, or nil for a tombstone.
	memtable := map[string]*string{}

	n.OnStart(func(ctx context.Context) {
		hasLog, err := n.Disk().Get("log", &wal)
		if err != nil {
			log.Fatalf("reading the log: %v", err)
		}
		if _, err := n.Disk().Get("segments", &segments); err != nil {
			log.Fatalf("reading the segments: %v", err)
		}
		// The segments are already on disk; the memtable comes back from its log.
		for _, e := range wal {
			memtable[e.Key] = e.Value
		}
		if !hasLog {
			err := n.Disk().Update(func(tx *quorumlab.Tx) error {
				if err := tx.Set("log", wal); err != nil {
					return err
				}
				return tx.Set("segments", segments)
			})
			if err != nil {
				log.Fatalf("creating the disk: %v", err)
			}
		}
	})

	// TODO: turn the memtable into a segment: its entries sorted by key
	//       (sorted(memtable)), appended to segments on disk. Then empty the
	//       memtable and the log, in the same n.Disk().Update.
	// flush := func() error { return nil }

	// write logs first, then updates the memtable; the caller replies after.
	write := func(key string, value *string) error {
		mu.Lock()
		defer mu.Unlock()
		next := append(wal, pair{key, value})
		if err := n.Disk().Set("log", next); err != nil {
			return err
		}
		wal = next
		memtable[key] = value
		// TODO: flush once the memtable has taken memtableLimit writes.
		return nil
	}

	// lookup: the caller holds mu.
	lookup := func(key string) *string {
		if value, ok := memtable[key]; ok {
			return value
		}
		// TODO: search the segments, newest first. The first one that has the
		//       key decides, even if what it holds is a tombstone.
		return nil
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if err := write(req.Key, &req.Value); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("delete", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: removing the key from the memtable is not enough: an older value
		//       may sit in a segment. Write a tombstone (a nil value) instead.
		mu.Lock()
		delete(memtable, req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "delete_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := lookup(req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	// TODO: n.Handle("scan", ...) -> {"type": "scan_ok", "entries": [[key, value], ...]}
	//       for from <= key < to, in key order, without deleted keys.

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
