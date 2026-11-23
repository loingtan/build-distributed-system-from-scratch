// Replicate the log. Read the brief: npm run lab -- brief raft-replicate
//
// This is the leader election solution. Elections already work.
// Your job: make the leader's log appear, in the same order, on every node.
//
// Every method on raft is called with r.mu held. Never hold r.mu across
// n.Call: send from a goroutine and take the lock again for the answer.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// Entry is one log entry. Index 1 is log[0].
type Entry struct {
	Term  int `json:"term"`
	Value any `json:"value"`
}

type raft struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	term     int
	role     string // "follower", "candidate" or "leader"
	votedFor string // "" for nobody
	leader   string // "" if unknown
	votes    int

	electionTimer *quorumlab.Timer
	timerGen      int

	log       []Entry        // index 1 is log[0]
	nextIndex map[string]int // leader only: the next index to send to each peer
}

func main() {
	n := quorumlab.NewNode()
	r := &raft{n: n, role: "follower", log: []Entry{}, nextIndex: map[string]int{}}

	n.Handle("request_vote", r.handleRequestVote)
	n.Handle("heartbeat", r.handleHeartbeat)
	n.Handle("append", r.handleAppend)
	n.Handle("log", r.handleLog)
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

func (r *raft) resetElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
	gen := r.timerGen
	timeout := time.Duration((300 + 300*r.n.Rand()) * float64(time.Millisecond))
	r.electionTimer = r.n.After(timeout, func(ctx context.Context) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if gen == r.timerGen {
			r.startElection(ctx)
		}
	})
}

func (r *raft) stopElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
}

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
	r.resetElectionTimer()

	ask := map[string]any{"type": "request_vote", "term": r.term}
	for _, peer := range r.n.Peers() {
		go r.requestVote(ctx, peer, ask, r.term)
	}
}

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
	// TODO: start nextIndex[peer] at len(r.log) + 1 for every peer.
	r.sendHeartbeats(ctx)
}

// TODO: turn the heartbeat into append_entries. For each peer send
//
//	{"type": "append_entries", "term": term, "prevIndex": prevIndex, "entries": entries}
//
// where entries is everything from nextIndex[peer] on and prevIndex is the
// index just before them. An empty entries list is still a heartbeat.
// Use n.Call (from a goroutine, with a copy of the entries) so you see the
// answer: on success move nextIndex[peer] forward; on failure move it back
// and send again.
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
	granted := req.Term == r.term && (r.votedFor == "" || r.votedFor == msg.Src)
	if granted {
		r.votedFor = msg.Src
		r.resetElectionTimer()
	}
	return r.n.Reply(msg, map[string]any{"type": "request_vote_ok", "term": r.term, "granted": granted})
}

// TODO: a follower receiving append_entries does everything the heartbeat
// handler does, and then:
//   - if its log is shorter than prevIndex, it refuses (there would be a hole)
//   - otherwise it stores the entries it does not have yet, at their indexes
//   - it replies with success and how far its log now matches the leader's
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
		return nil
	}
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	r.role = "follower"
	r.leader = msg.Src
	r.resetElectionTimer()
	return nil
}

func (r *raft) handleAppend(ctx context.Context, msg quorumlab.Message) error {
	// TODO: if this node is not the leader, reply {"type": "not_leader", "leader": leader}.
	// Otherwise append Entry{Term: term, Value: value} to the log,
	// reply {"type": "append_ok"} and start replicating.
	return nil
}

func (r *raft) handleLog(ctx context.Context, msg quorumlab.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "log_ok", "entries": r.log})
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
