// Raft leader election: terms, one vote per term, randomised election
// timeouts, and heartbeats from the leader.
//
// All state lives in the raft struct, guarded by r.mu. Every method that
// touches it is called with r.mu held. Votes are requested from goroutines,
// because r.mu must never be held across n.Call.
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
	votes    int    // votes received in the current election

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
		r.resetElectionTimer()
		r.mu.Unlock()

		n.Every(100*time.Millisecond, func(ctx context.Context) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.role == "leader" {
				r.sendHeartbeats(ctx)
			}
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (r *raft) majority() int { return len(r.n.NodeIDs())/2 + 1 }

// resetElectionTimer (re)starts the countdown to an election. The timeout is
// random so nodes rarely tie.
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

func (r *raft) stopElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
}

// adoptTerm is the one rule that keeps the cluster consistent: a higher term
// always wins.
func (r *raft) adoptTerm(term int) {
	r.term = term
	r.role = "follower"
	r.votedFor = ""
	r.leader = ""
	r.resetElectionTimer()
}

func (r *raft) startElection(ctx context.Context) {
	r.term++
	r.role = "candidate"
	r.votedFor = r.n.ID()
	r.leader = ""
	r.votes = 1
	r.resetElectionTimer() // if this election stalls, try again later

	ask := map[string]any{"type": "request_vote", "term": r.term}
	for _, peer := range r.n.Peers() {
		go r.requestVote(ctx, peer, ask, r.term)
	}
}

// requestVote asks one peer for its vote. It runs in its own goroutine and
// takes r.mu only once the answer is in.
func (r *raft) requestVote(ctx context.Context, peer string, ask map[string]any, electionTerm int) {
	res, err := r.n.Call(ctx, peer, ask, 200*time.Millisecond)
	if err != nil {
		return
	}
	var vote struct {
		Term    int  `json:"term"`
		Granted bool `json:"granted"`
	}
	if err := res.Decode(&vote); err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if vote.Term > r.term {
		r.adoptTerm(vote.Term)
		return
	}
	// Ignore answers to an election we have already moved on from.
	if r.role != "candidate" || r.term != electionTerm || !vote.Granted {
		return
	}
	r.votes++
	if r.votes >= r.majority() {
		r.becomeLeader(ctx)
	}
}

func (r *raft) becomeLeader(ctx context.Context) {
	r.role = "leader"
	r.leader = r.n.ID()
	r.stopElectionTimer()
	r.sendHeartbeats(ctx)
}

func (r *raft) sendHeartbeats(ctx context.Context) {
	for _, peer := range r.n.Peers() {
		r.n.Send(ctx, peer, map[string]any{"type": "heartbeat", "term": r.term})
	}
}

func (r *raft) handleRequestVote(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Term int `json:"term"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	// One vote per term: this is what makes two leaders in a term impossible.
	granted := req.Term == r.term && (r.votedFor == "" || r.votedFor == msg.Src)
	if granted {
		r.votedFor = msg.Src
		r.resetElectionTimer()
	}
	return r.n.Reply(msg, map[string]any{"type": "request_vote_ok", "term": r.term, "granted": granted})
}

func (r *raft) handleHeartbeat(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Term int `json:"term"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term < r.term {
		return nil // a deposed leader that has not noticed yet
	}
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	r.role = "follower"
	r.leader = msg.Src
	r.resetElectionTimer()
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
