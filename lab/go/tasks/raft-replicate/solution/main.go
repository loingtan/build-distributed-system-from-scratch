// Raft log replication: the leader keeps nextIndex for every follower and
// keeps sending it everything from there on. The same message is the
// heartbeat.
//
// Every method on raft is called with r.mu held. RPCs are sent from
// goroutines, with a copy of the entries, because r.mu must never be held
// across n.Call.
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

// appendEntries is the leader's request; the follower decodes the same struct.
type appendEntries struct {
	Type      string  `json:"type"`
	Term      int     `json:"term"`
	PrevIndex int     `json:"prevIndex"`
	Entries   []Entry `json:"entries"`
}

type appendEntriesReply struct {
	Term       int  `json:"term"`
	Success    bool `json:"success"`
	MatchIndex int  `json:"matchIndex"`
	LogLength  int  `json:"logLength"`
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
	n.Handle("append_entries", r.handleAppendEntries)
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
				r.replicate(ctx)
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
	// Assume every follower is up to date; their answers will correct us.
	for _, peer := range r.n.Peers() {
		r.nextIndex[peer] = len(r.log) + 1
	}
	r.replicate(ctx)
}

func (r *raft) replicate(ctx context.Context) {
	for _, peer := range r.n.Peers() {
		r.sendEntries(ctx, peer)
	}
}

// sendEntries sends one follower everything from its nextIndex on. With
// nothing new to send this is an empty message, which is the heartbeat.
func (r *raft) sendEntries(ctx context.Context, peer string) {
	sentTerm := r.term
	prevIndex := r.nextIndex[peer] - 1
	req := appendEntries{
		Type:      "append_entries",
		Term:      r.term,
		PrevIndex: prevIndex,
		Entries:   append([]Entry{}, r.log[prevIndex:]...), // a copy: r.log changes once we unlock
	}
	go func() {
		res, err := r.n.Call(ctx, peer, req, 100*time.Millisecond)
		if err != nil {
			return // lost: the next tick sends it again
		}
		var reply appendEntriesReply
		if err := res.Decode(&reply); err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if reply.Term > r.term {
			r.adoptTerm(reply.Term)
			return
		}
		if r.role != "leader" || r.term != sentTerm {
			return
		}
		if reply.Success {
			// Replies can arrive out of order; never move backwards on a success.
			r.nextIndex[peer] = max(r.nextIndex[peer], reply.MatchIndex+1)
		} else {
			// The follower has a gap before prevIndex. Go back to what it has.
			r.nextIndex[peer] = min(r.nextIndex[peer], reply.LogLength+1)
			r.sendEntries(ctx, peer)
		}
	}()
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

func (r *raft) handleAppendEntries(ctx context.Context, msg quorumlab.Message) error {
	var req appendEntries
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term < r.term {
		return r.refuseEntries(msg)
	}
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	r.role = "follower"
	r.leader = msg.Src
	r.resetElectionTimer()

	// The entries start right after prevIndex. If we do not have that much
	// yet, storing them would leave a hole.
	if len(r.log) < req.PrevIndex {
		return r.refuseEntries(msg)
	}
	// Messages can be duplicated or overtaken: store only what is new.
	for i, entry := range req.Entries {
		if index := req.PrevIndex + 1 + i; index > len(r.log) {
			r.log = append(r.log, entry)
		}
	}
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": req.PrevIndex + len(req.Entries)})
}

// refuseEntries answers an append_entries that is stale or would leave a
// hole, with our term and how long our log is.
func (r *raft) refuseEntries(msg quorumlab.Message) error {
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": false, "logLength": len(r.log)})
}

func (r *raft) handleAppend(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != "leader" {
		return r.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(r.leader)})
	}
	r.log = append(r.log, Entry{Term: r.term, Value: req.Value})
	if err := r.n.Reply(msg, map[string]any{"type": "append_ok"}); err != nil {
		return err
	}
	r.replicate(ctx)
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
