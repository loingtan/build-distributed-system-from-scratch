// Bloom filters. Read the brief: npm run lab -- brief store-bloom
//
// The compacting LSM tree from the previous task, plus a counter of segment
// searches. A get for a key that does not exist searches every segment.
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

const (
	maxSegments   = 4
	memtableLimit = 8 // writes
)

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

// segment is { "entries": [[key, value], ...] }, entries sorted by key.
type segment struct {
	Entries []pair `json:"entries"`
}

// search finds key by binary search: this is what sorting the segment buys.
// found is false if the key is not here; a found nil value is a tombstone.
func (s segment) search(key string) (value *string, found bool) {
	i, ok := slices.BinarySearchFunc(s.Entries, key, func(e pair, key string) int { return strings.Compare(e.Key, key) })
	if !ok {
		return nil, false
	}
	return s.Entries[i].Value, true
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

	// How many times a get has searched a segment's entries since startup.
	segmentReads := 0

	// TODO: build a Bloom filter over a segment's keys when the segment is
	//       written (in flush and in compact), and keep it in the segment
	//       struct beside Entries, for example Bloom []int `json:"bloom"`,
	//       so that it is saved to disk with the segment.
	// TODO: before searching a segment, ask its filter. "Definitely not here"
	//       means skip the segment, without counting a read.

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

	// flush turns the memtable into a segment: sorted, and never changed
	// again. Its contents are durable in the segment, so the same disk write
	// starts the log afresh.
	flush := func() error {
		next := append(segments, segment{Entries: sorted(memtable)})
		err := n.Disk().Update(func(tx *quorumlab.Tx) error {
			if err := tx.Set("segments", next); err != nil {
				return err
			}
			return tx.Set("log", []pair{})
		})
		if err != nil {
			return err
		}
		segments, wal = next, []pair{}
		clear(memtable)
		return nil
	}

	// compact merges every segment into one. Going from oldest to newest, a
	// later entry for a key replaces an earlier one, so only the newest
	// survives. Because the merge includes the oldest segment, nothing older
	// is left for a tombstone to hide, and tombstones can be dropped too.
	// The new list replaces the old one in a single write: a crash leaves
	// either all the old segments or the merged one, never a mixture.
	compact := func() error {
		merged := map[string]*string{}
		for _, s := range segments {
			for _, e := range s.Entries {
				merged[e.Key] = e.Value
			}
		}
		for key, value := range merged {
			if value == nil {
				delete(merged, key)
			}
		}
		next := []segment{segment{Entries: sorted(merged)}}
		if err := n.Disk().Set("segments", next); err != nil {
			return err
		}
		segments = next
		return nil
	}

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
		if len(wal) < memtableLimit {
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		if len(segments) >= maxSegments {
			return compact()
		}
		return nil
	}

	// lookup: newest data wins: memtable, then segments from the last flushed
	// to the first. A tombstone is an answer ("deleted"), so the search stops
	// there. The caller holds mu.
	lookup := func(key string) *string {
		if value, ok := memtable[key]; ok {
			return value
		}
		for i := len(segments) - 1; i >= 0; i-- {
			segmentReads++
			if value, found := segments[i].search(key); found {
				return value
			}
		}
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
		if err := write(req.Key, nil); err != nil {
			return err
		}
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

	// scan overlays everything from oldest to newest so later writes replace
	// earlier ones, then drops the tombstones and sorts.
	n.Handle("scan", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		found := map[string]*string{}
		take := func(key string, value *string) {
			if key >= req.From && key < req.To {
				found[key] = value
			}
		}
		for _, s := range segments {
			for _, e := range s.Entries {
				take(e.Key, e.Value)
			}
		}
		for key, value := range memtable {
			take(key, value)
		}
		mu.Unlock()
		for key, value := range found {
			if value == nil {
				delete(found, key)
			}
		}
		return n.Reply(msg, map[string]any{"type": "scan_ok", "entries": sorted(found)})
	})

	n.Handle("stats", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "stats_ok", "segment_reads": segmentReads})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
