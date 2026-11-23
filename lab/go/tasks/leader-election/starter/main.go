// Leader election. Read the brief: npm run lab -- brief leader-election
//
// Raft leader election. The state every node keeps lives in the raft struct.
// Handlers and timers run concurrently, so every method below that touches it
// is called with r.mu held. Never hold r.mu across n.Call: ask peers from a
// goroutine, and take the lock again when the answer arrives.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type raft struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	term     int    // the latest term this node has seen
	role     string // "follower", "candidate" or "leader"
	votedFor string // who this node voted for in term ("" for nobody)
	leader   string // who this node believes leads term ("" if unknown)

	electionTimer *quorumlab.Timer
	timerGen      int // bumped on every reset, so a timer that already fired knows it is stale
}

func main() {
	n := quorumlab.NewNode()
	r := &raft{n: n, role: "follower"}

	n.Handle("request_vote", r.handleRequestVote)
	n.Handle("heartbeat", r.handleHeartbeat)
	n.Handle("status", r.handleStatus)

	n.OnStart(func(ctx context.Context) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.resetElectionTimer()

		// TODO: while leader, send {"type": "heartbeat", "term": term} to every
		// peer regularly (n.Every), more often than the election timeout.
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (r *raft) majority() int { return len(r.n.NodeIDs())/2 + 1 }

// resetElectionTimer (re)starts the countdown to an election. Call it
// whenever a valid leader is heard from. The timeout is random so nodes
// rarely tie.
func (r *raft) resetElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
	gen := r.timerGen
	timeout := time.Duration((300 + 300*r.n.Rand()) * float64(time.Millisecond))
	r.electionTimer = r.n.After(timeout, func(ctx context.Context) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if gen == r.timerGen { // not reset or stopped since it was set
			r.startElection(ctx)
		}
	})
}

func (r *raft) startElection(ctx context.Context) {
	// TODO: term += 1, become a candidate, vote for yourself,
	// restart the timer, then ask every peer (from a goroutine):
	//   res, err := r.n.Call(ctx, peer, map[string]any{"type": "request_vote", "term": term}, 200*time.Millisecond)
	// With votes from a majority (in the same term), become leader.
}

func (r *raft) handleRequestVote(ctx context.Context, msg quorumlab.Message) error {
	// TODO: if the request's term is newer than ours, adopt it and step down.
	// Grant the vote only for the current term, and only once per term.
	// Reply {"type": "request_vote_ok", "term": term, "granted": granted}.
	return nil
}

func (r *raft) handleHeartbeat(ctx context.Context, msg quorumlab.Message) error {
	// TODO: ignore heartbeats from an older term. Otherwise accept the
	// sender as leader and reset the election timer.
	return nil
}

func (r *raft) handleStatus(ctx context.Context, msg quorumlab.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "status_ok", "role": r.role, "term": r.term, "leader": orNull(r.leader)})
}

// orNull turns "" (nobody) into JSON null.
func orNull(id string) any {
	if id == "" {
		return nil
	}
	return id
}
