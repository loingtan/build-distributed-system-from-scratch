// Crash recovery: two-phase commit with a log. Everything a node has told
// another node is on disk before the message leaves, and a restarted node
// finishes whatever its log says is still open.
package main

import (
	"context"
	"log"
	"slices"
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

// decision is a coordinator's outcome for one transaction, kept until every
// participant has acknowledged it.
type decision struct {
	Commit  bool     `json:"commit"`
	Waiting []string `json:"waiting"` // participants that have not acknowledged
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

	// Everything a node must still know after a crash lives on disk, one key
	// per map. mu guards all of it, and is held while it is written.
	var mu sync.Mutex
	balances := map[string]int{"a": 100, "b": 100}
	committed := []string{}           // ids of the transactions this node has applied
	aborted := map[string]bool{}      // ids this node has been told were aborted
	prepared := map[string][]move{}   // participant: txn -> its moves, voted yes, outcome unknown
	started := map[string][]string{}  // coordinator: txn -> participants, no decision yet
	decided := map[string]*decision{} // coordinator: txn -> outcome and who still has to hear it

	// Locks are not stored: they follow from the prepared transactions.
	locks := map[string]string{} // account -> the prepared txn that holds it

	// save writes the whole state in one atomic step and returns once it is
	// on disk. The caller holds mu, and sends nothing that depends on the
	// change until save has returned.
	save := func() error {
		return n.Disk().Update(func(tx *quorumlab.Tx) error {
			for key, v := range map[string]any{
				"balances": balances, "committed": committed, "aborted": aborted,
				"prepared": prepared, "started": started, "decided": decided,
			} {
				if err := tx.Set(key, v); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// ---- coordinator

	// tell delivers the outcome to one participant, however long it takes,
	// then crosses it off the decision's waiting list.
	tell := func(ctx context.Context, participant, txn string, commit bool) {
		outcome := "abort"
		if commit {
			outcome = "commit"
		}
		for {
			if _, err := n.Call(ctx, participant, map[string]any{"type": outcome, "txn": txn}, retryTimeout); err == nil {
				break
			}
		}
		mu.Lock()
		defer mu.Unlock()
		d := decided[txn]
		if d == nil {
			return
		}
		d.Waiting = slices.DeleteFunc(d.Waiting, func(p string) bool { return p == participant })
		if len(d.Waiting) == 0 {
			delete(decided, txn)
		}
		if err := save(); err != nil {
			n.Log("saving: %v", err)
		}
	}

	// finish sends a decision to everyone still waiting for it. The caller
	// holds mu.
	finish := func(ctx context.Context, txn string) {
		d := decided[txn]
		for _, p := range d.Waiting {
			go tell(ctx, p, txn, d.Commit)
		}
	}

	// decide makes the decision. It exists once it is on disk, and only then
	// is it sent. The caller holds mu.
	decide := func(ctx context.Context, txn string, commit bool) error {
		decided[txn] = &decision{Commit: commit, Waiting: started[txn]}
		delete(started, txn)
		if err := save(); err != nil {
			return err
		}
		finish(ctx, txn)
		return nil
	}

	n.OnStart(func(ctx context.Context) {
		// After a restart, read everything back. A key that was never
		// written keeps its starting value.
		for key, v := range map[string]any{
			"balances": &balances, "committed": &committed, "aborted": &aborted,
			"prepared": &prepared, "started": &started, "decided": &decided,
		} {
			if _, err := n.Disk().Get(key, v); err != nil {
				log.Fatalf("reading %s from disk: %v", key, err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for txn, moves := range prepared {
			for _, m := range moves {
				locks[m.Account] = txn
			}
		}
		// Recovery. A transaction that was started but never decided cannot
		// have committed anywhere, so it is safe to abort it now. A decided one
		// is simply sent out again to whoever has not acknowledged.
		for txn := range started {
			if err := decide(ctx, txn, false); err != nil {
				log.Fatalf("recovering %s: %v", txn, err)
			}
		}
		for txn := range decided {
			finish(ctx, txn)
		}
	})

	n.Handle("transfer", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		ps := participants(req.Moves)

		// Written before the first prepare leaves, so that after a crash this
		// node knows whom it may have left prepared.
		mu.Lock()
		started[req.Txn] = ps
		err := save()
		mu.Unlock()
		if err != nil {
			return err
		}

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

		mu.Lock()
		err = decide(ctx, req.Txn, commit)
		mu.Unlock()
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "transfer_ok", "txn": req.Txn, "committed": commit})
	})

	// ---- participant

	n.Handle("prepare", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn   string `json:"txn"`
			Moves []move `json:"moves"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
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
			// On disk first, then the vote: a yes must survive a crash.
			prepared[req.Txn] = req.Moves
			for _, m := range req.Moves {
				locks[m.Account] = req.Txn
			}
			if err := save(); err != nil {
				mu.Unlock()
				return err
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
			if err := save(); err != nil {
				mu.Unlock()
				return err
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

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
		err := save()
		mu.Unlock()
		if err != nil {
			return err
		}
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
