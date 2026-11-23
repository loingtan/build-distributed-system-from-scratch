// Prepare, then commit. Read the brief: npm run lab -- brief tpc-prepare-commit
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// move is one change to one account on one node.
type move struct {
	Node    string `json:"node"`
	Account string `json:"account"`
	Delta   int    `json:"delta"`
}

// participants lists the nodes named in moves, each once, in order.
func participants(moves []move) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range moves {
		if !seen[m.Node] {
			seen[m.Node] = true
			out = append(out, m.Node)
		}
	}
	return out
}

// movesFor keeps the moves on one node.
func movesFor(moves []move, node string) []move {
	var out []move
	for _, m := range moves {
		if m.Node == node {
			out = append(out, m)
		}
	}
	return out
}

func main() {
	n := quorumlab.NewNode()

	// Every node is a bank branch with two accounts.
	var mu sync.Mutex
	balances := map[string]int{"a": 100, "b": 100}
	committed := []string{} // ids of the transactions this node has applied

	// A client sends "transfer" to any node. That node is the coordinator for
	// this transaction; the nodes named in the moves are its participants.
	// (A node can send messages to itself, so the coordinator may treat itself
	// like any other participant.)
	n.Handle("transfer", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		ps := participants(req.Moves)

		// One phase: tell every participant to apply its moves, and hope.
		// TODO: this is the bug. A participant that can apply does so at once,
		// even when another participant refuses. Split it into two rounds:
		//   1. "prepare": each participant checks its moves and votes yes or no,
		//      without changing any balance.
		//   2. "commit" to everyone if every vote was yes, otherwise "abort".
		applied := make([]bool, len(ps))
		var wg sync.WaitGroup
		for i, p := range ps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := n.Call(ctx, p, map[string]any{"type": "apply", "txn": req.Txn, "moves": movesFor(req.Moves, p)}, time.Second)
				var a struct {
					Applied bool `json:"applied"`
				}
				applied[i] = err == nil && res.Decode(&a) == nil && a.Applied
			}()
		}
		wg.Wait()
		ok := true
		for _, a := range applied {
			ok = ok && a
		}
		return n.Reply(msg, map[string]any{"type": "transfer_ok", "txn": req.Txn, "committed": ok})
	})

	n.Handle("apply", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		fits := true
		for _, m := range req.Moves {
			if balances[m.Account]+m.Delta < 0 {
				fits = false
			}
		}
		if fits {
			for _, m := range req.Moves {
				balances[m.Account] += m.Delta
			}
			committed = append(committed, req.Txn)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "apply_ok", "applied": fits})
	})

	n.Handle("status", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "status_ok", "balances": balances, "committed": committed})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
