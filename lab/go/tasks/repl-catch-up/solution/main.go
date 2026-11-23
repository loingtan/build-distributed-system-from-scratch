// Catching up: an ordered log on disk, and one sender per backup that pushes
// whatever the backup is missing, in order, until it has everything.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// entry is one write. Its position in the log is its sequence number.
type entry struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func main() {
	n := quorumlab.NewNode()

	// The first node is the primary; it never changes.
	var primary string
	var isPrimary bool

	// Every write, in the primary's order. An entry's position is its sequence
	// number, so "how many entries do you have?" is all a backup needs to say
	// for the primary to know what it is missing. The log lives on disk, under
	// the key "log".
	var mu sync.Mutex
	var writes []entry
	// The store is just the log replayed, so it can be rebuilt after a restart.
	store := map[string]any{}

	// appendWrites extends the log: on disk first, then in memory, so nothing
	// is served or acknowledged that a crash could take away. The caller
	// holds mu, which also keeps two appends from racing to the disk.
	appendWrites := func(batch []entry) error {
		next := append(writes, batch...)
		if err := n.Disk().Set("log", next); err != nil {
			return err
		}
		writes = next
		for _, e := range batch {
			store[e.Key] = e.Value
		}
		return nil
	}

	// ---- primary: one sender per backup

	have := map[string]int{}     // backup -> how many log entries it has confirmed
	sending := map[string]bool{} // backup -> is a sender loop running?

	catchUp := func(ctx context.Context, peer string) {
		mu.Lock()
		defer mu.Unlock()
		if sending[peer] {
			return
		}
		sending[peer] = true
		for have[peer] < len(writes) {
			from := have[peer]
			batch := writes[from:min(from+50, len(writes))] // never modified: appends only add after it
			mu.Unlock()
			res, err := n.Call(ctx, peer, map[string]any{"type": "replicate", "from": from, "entries": batch}, 100*time.Millisecond)
			mu.Lock()
			// No answer: lost, partitioned or crashed. Just ask again.
			var ok struct {
				Have int `json:"have"`
			}
			if err == nil && res.Decode(&ok) == nil {
				have[peer] = ok.Have
			}
		}
		// Checked and cleared under the same lock, so a write that arrives
		// now either sees the loop running or starts a new one.
		sending[peer] = false
	}

	n.OnStart(func(ctx context.Context) {
		primary = n.NodeIDs()[0]
		isPrimary = n.ID() == primary

		// Crash recovery is this one read: the log from disk, replayed.
		if _, err := n.Disk().Get("log", &writes); err != nil {
			log.Fatalf("reading the log from disk: %v", err)
		}
		for _, e := range writes {
			store[e.Key] = e.Value
		}

		// After a restart the primary does not know what its backups have.
		// Sending from 0 is safe: each backup answers with its real position.
		if isPrimary {
			for _, peer := range n.Peers() {
				go catchUp(ctx, peer)
			}
		}
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}

		if !isPrimary {
			res, err := n.Call(ctx, primary, map[string]any{"type": "write", "key": req.Key, "value": req.Value}, 500*time.Millisecond)
			if err != nil {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			return n.Reply(msg, map[string]any{"type": res.Type()})
		}

		mu.Lock()
		err := appendWrites([]entry{{Key: req.Key, Value: req.Value}}) // on disk before the client hears write_ok
		mu.Unlock()
		if err != nil {
			return err
		}
		if err := n.Reply(msg, map[string]any{"type": "write_ok"}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			go catchUp(ctx, peer)
		}
		return nil
	})

	// ---- backup

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			From    int     `json:"from"`
			Entries []entry `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		// Take only what extends the log exactly: skip entries we already have,
		// and take nothing if there would be a gap.
		if skip := len(writes) - req.From; skip >= 0 && skip < len(req.Entries) {
			if err := appendWrites(req.Entries[skip:]); err != nil {
				mu.Unlock()
				return err
			}
		}
		count := len(writes)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "replicate_ok", "have": count})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
