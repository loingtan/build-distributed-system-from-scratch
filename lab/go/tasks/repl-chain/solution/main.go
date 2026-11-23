// Chain replication: writes enter at the head, flow down the chain in the
// head's order, and are acknowledged by the tail, which also serves reads.
package main

import (
	"context"
	"log"
	"slices"
	"sync"
	"time"

	"quorumlab"
)

// entry is one write, in the head's order. It carries who asked, so the tail
// can answer.
type entry struct {
	Key    string `json:"key"`
	Value  any    `json:"value"`
	Client string `json:"client"` // who asked
	MsgID  int64  `json:"msg_id"` // the msg_id of their request
}

func main() {
	n := quorumlab.NewNode()

	// The chain is n.NodeIDs() in order: the first node is the head, the last
	// is the tail. next is "" at the tail. Node ids are known once the node
	// has started.
	var head, tail, next string
	n.OnStart(func(ctx context.Context) {
		chain := n.NodeIDs()
		head, tail = chain[0], chain[len(chain)-1]
		if i := slices.Index(chain, n.ID()); i+1 < len(chain) {
			next = chain[i+1]
		}
	})

	var mu sync.Mutex
	var writes []entry        // writes in the head's order
	store := map[string]any{} // key -> value
	confirmed := 0            // how many log entries next has confirmed
	sending := false          // is the passOn loop running?

	// One loop per node pushes the log to its successor, in order, and
	// repeats anything that was not confirmed.
	passOn := func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		if sending {
			return
		}
		sending = true
		for confirmed < len(writes) {
			from := confirmed
			batch := writes[from:] // never modified: appends only add after it
			mu.Unlock()
			res, err := n.Call(ctx, next, map[string]any{"type": "chain", "from": from, "entries": batch}, 150*time.Millisecond)
			mu.Lock()
			var ok struct {
				Have int `json:"have"`
			}
			if err == nil && res.Decode(&ok) == nil {
				confirmed = ok.Have
			}
		}
		sending = false
	}

	// appendWrites adds entries to the log. The caller holds mu.
	appendWrites := func(batch []entry) {
		writes = append(writes, batch...)
		for _, e := range batch {
			store[e.Key] = e.Value
		}
	}

	// handOn runs after new entries were appended, without mu held.
	handOn := func(ctx context.Context, added []entry) {
		if n.ID() != tail {
			go passOn(ctx)
			return
		}
		// Every node before the tail already has these entries, so it is safe
		// to tell the clients. This is n.Reply done by hand.
		for _, e := range added {
			n.Send(ctx, e.Client, map[string]any{"type": "write_ok", "in_reply_to": e.MsgID})
		}
	}

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		if n.ID() != head {
			return n.Reply(msg, map[string]any{"type": "error", "code": "not_head", "head": head})
		}
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
			MsgID int64  `json:"msg_id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// The entry carries who asked, so the tail can answer.
		added := []entry{{Key: req.Key, Value: req.Value, Client: msg.Src, MsgID: req.MsgID}}
		mu.Lock()
		appendWrites(added)
		mu.Unlock()
		handOn(ctx, added)
		return nil
	})

	n.Handle("chain", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			From    int     `json:"from"`
			Entries []entry `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// Take only entries that extend the log exactly; a repeat adds nothing.
		var added []entry
		mu.Lock()
		if skip := len(writes) - req.From; skip >= 0 && skip < len(req.Entries) {
			added = req.Entries[skip:]
			appendWrites(added)
		}
		count := len(writes)
		mu.Unlock()
		if len(added) > 0 {
			handOn(ctx, added)
		}
		return n.Reply(msg, map[string]any{"type": "chain_ok", "have": count})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		if n.ID() != tail {
			return n.Reply(msg, map[string]any{"type": "error", "code": "not_tail", "tail": tail})
		}
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
