// Chain replication. Read the brief: npm run lab -- brief repl-chain
//
// The chain is n.NodeIDs() in order: the first node is the head, the last is
// the tail.
package main

import (
	"context"
	"log"
	"slices"
	"sync"

	"quorumlab"
)

// entry is one write, in the head's order.
type entry struct {
	Key    string `json:"key"`
	Value  any    `json:"value"`
	Client string `json:"client"` // who asked
	MsgID  int64  `json:"msg_id"` // the msg_id of their request
}

func main() {
	n := quorumlab.NewNode()

	// Node ids are known once the node has started. next is "" at the tail.
	var head, tail, next string
	n.OnStart(func(ctx context.Context) {
		chain := n.NodeIDs()
		head, tail = chain[0], chain[len(chain)-1]
		if i := slices.Index(chain, n.ID()); i+1 < len(chain) {
			next = chain[i+1]
		}
	})
	_ = next // Go rejects unused variables; delete this line once you use next

	var mu sync.Mutex
	var writes []entry        // writes in the head's order
	store := map[string]any{} // key -> value

	// The caller holds mu.
	appendWrite := func(e entry) {
		writes = append(writes, e)
		store[e.Key] = e.Value
		// TODO: if this node is the tail, the write is now on every node:
		// answer the client that sent it. n.Reply needs the original message,
		// which only the head has, so do by hand what n.Reply does:
		//   n.Send(ctx, e.Client, map[string]any{"type": "write_ok", "in_reply_to": e.MsgID})
		// Otherwise: make sure next gets this entry, after all earlier ones.
		// (Release mu before n.Send or n.Call.)
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
		mu.Lock()
		appendWrite(entry{Key: req.Key, Value: req.Value, Client: msg.Src, MsgID: req.MsgID})
		mu.Unlock()
		// TODO: the head must not answer. Remove this once the tail does.
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	// TODO: a handler for entries arriving from the previous node. They may be
	// lost, repeated or out of order; the log must not be.

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
