// Concurrent transfers: two-phase commit where a yes vote locks the accounts
// it promised, until the outcome arrives.
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

	// Every node is a bank branch with two accounts. One mutex guards it all:
	// a prepare's check and its locking must happen as one step.
	var mu sync.Mutex
	balances := map[string]int{"a": 100, "b": 100}
	committed := []string{}         // ids of the transactions this node has applied
	prepared := map[string][]move{} // txn -> this node's moves, voted yes, waiting for the outcome
	locks := map[string]string{}    // account -> the prepared txn that holds it

	// ---- coordinator

	n.Handle("transfer", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		ps := participants(req.Moves)

		// Phase 1: ask everyone at once. A missing answer counts as no.
		votes := make([]bool, len(ps))
		var wg sync.WaitGroup
		for i, p := range ps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := n.Call(ctx, p, map[string]any{"type": "prepare", "txn": req.Txn, "moves": movesFor(req.Moves, p)}, time.Second)
				var v struct {
					Vote string `json:"vote"`
				}
				votes[i] = err == nil && res.Decode(&v) == nil && v.Vote == "yes"
			}()
		}
		wg.Wait()
		commit := true
		for _, yes := range votes {
			commit = commit && yes
		}

		// Phase 2: one decision, sent to everyone, including those who voted no.
		outcome := "abort"
		if commit {
			outcome = "commit"
		}
		for _, p := range ps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n.Call(ctx, p, map[string]any{"type": outcome, "txn": req.Txn}, time.Second)
			}()
		}
		wg.Wait()
		return n.Reply(msg, map[string]any{"type": "transfer_ok", "txn": req.Txn, "committed": commit})
	})

	// ---- participant

	// A yes vote is a promise that the moves can still be applied later. So the
	// accounts are locked from the vote until the outcome arrives, and a second
	// transaction that wants a locked account is refused straight away. Refusing
	// instead of waiting means two transactions can never wait for each other.
	n.Handle("prepare", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		free, fits := true, true
		for _, m := range req.Moves {
			if _, locked := locks[m.Account]; locked {
				free = false
			}
			if balances[m.Account]+m.Delta < 0 {
				fits = false
			}
		}
		if free && fits {
			prepared[req.Txn] = req.Moves
			for _, m := range req.Moves {
				locks[m.Account] = req.Txn
			}
		}
		mu.Unlock()
		vote := "no"
		if free && fits {
			vote = "yes"
		}
		return n.Reply(msg, map[string]any{"type": "prepare_ok", "vote": vote})
	})

	// release forgets a prepared transaction and unlocks its accounts. The
	// caller holds mu.
	release := func(txn string) []move {
		moves, ok := prepared[txn]
		if !ok {
			return nil
		}
		delete(prepared, txn)
		for _, m := range moves {
			delete(locks, m.Account)
		}
		return moves
	}

	n.Handle("commit", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn string `json:"txn"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		if moves := release(req.Txn); moves != nil {
			for _, m := range moves {
				balances[m.Account] += m.Delta
			}
			committed = append(committed, req.Txn)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

	// An abort also reaches nodes that voted no: release() then finds nothing.
	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn string `json:"txn"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		release(req.Txn)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
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
