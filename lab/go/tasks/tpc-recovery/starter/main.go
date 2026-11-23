// Crash recovery. Read the brief: npm run lab -- brief tpc-recovery
//
// This is two-phase commit that survives a broken network, but keeps
// everything in memory. A crash wipes it all.
//
// TODO: move what must survive onto n.Disk(), and write it BEFORE sending
// the message that depends on it:
//   - balances and committed ids
//   - participant: each prepared transaction (a yes vote is a promise)
//   - coordinator: which transactions it started and with whom, and each
//     decision until every participant has acknowledged it
//
// TODO: at startup (n.OnStart), after a restart: rebuild the locks from the
// prepared transactions, abort what was started but never decided, and send
// out again every decision that is still waiting for an acknowledgement.
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

const (
	prepareTimeout = 300 * time.Millisecond // a missing vote is a no after this
	retryTimeout   = 300 * time.Millisecond // resend the outcome after this
)

func main() {
	n := quorumlab.NewNode()

	// Every node is a bank branch with two accounts. One mutex guards it all:
	// a prepare's check and its locking must happen as one step.
	var mu sync.Mutex
	balances := map[string]int{"a": 100, "b": 100}
	committed := []string{}         // ids of the transactions this node has applied
	aborted := map[string]bool{}    // ids this node has been told were aborted
	prepared := map[string][]move{} // txn -> this node's moves, voted yes, waiting for the outcome
	locks := map[string]string{}    // account -> the prepared txn that holds it

	// ---- coordinator

	// tell delivers the outcome, however long it takes. A prepared participant
	// is waiting for exactly this message and cannot do anything else.
	tell := func(ctx context.Context, participant, txn string, commit bool) {
		outcome := "abort"
		if commit {
			outcome = "commit"
		}
		for {
			if _, err := n.Call(ctx, participant, map[string]any{"type": outcome, "txn": txn}, retryTimeout); err == nil {
				return
			}
		}
	}

	n.Handle("transfer", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		ps := participants(req.Moves)

		// Phase 1: ask everyone at once. A vote that does not arrive in time
		// counts as no. Before the decision the coordinator is free to give up;
		// nobody has been promised a commit.
		votes := make([]bool, len(ps))
		var wg sync.WaitGroup
		for i, p := range ps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := n.Call(ctx, p, map[string]any{"type": "prepare", "txn": req.Txn, "moves": movesFor(req.Moves, p)}, prepareTimeout)
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

		// This line is the decision. From here on it never changes, so the client
		// can be told now, without waiting for a participant that is cut off.
		if err := n.Reply(msg, map[string]any{"type": "transfer_ok", "txn": req.Txn, "committed": commit}); err != nil {
			return err
		}
		for _, p := range ps {
			go tell(ctx, p, req.Txn, commit)
		}
		return nil
	})

	// ---- participant

	// Note what is missing: a timer. After voting yes this node waits for the
	// coordinator, with its accounts locked, for as long as it takes.
	n.Handle("prepare", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		// A prepare that arrives after its own abort must not lock anything.
		free, fits := !aborted[req.Txn], true
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

	// The outcome may arrive more than once, because the coordinator retries
	// until it sees an acknowledgement. The second time release() finds nothing.
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

	// An abort also reaches nodes that voted no or never saw the prepare.
	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn string `json:"txn"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		release(req.Txn)
		aborted[req.Txn] = true
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
