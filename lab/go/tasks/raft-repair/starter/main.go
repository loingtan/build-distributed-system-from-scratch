// Repair diverged logs. Read the brief: npm run lab -- brief raft-repair
//
// This is the commit solution. It is correct while one leader stays in
// charge. After a partition and a leader change it is not:
//  1. a follower never checks that its log matches the leader's before
//     the new entries, and never removes entries the leader does not have
//  2. any node can win an election, even one that is missing committed entries
//  3. a new leader counts copies of entries from older terms as commits
//  4. seeing a higher term resets the election timer, so candidates that
//     can never win keep the one node that could win from standing
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
	Term  int    `json:"term"`
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type appendEntries struct {
	Type      string  `json:"type"`
	Term      int     `json:"term"`
	PrevIndex int     `json:"prevIndex"`
	Entries   []Entry `json:"entries"`
	Commit    int     `json:"commit"`
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

	log         []Entry                   // index 1 is log[0]
	commitIndex int                       // entries 1..commitIndex are committed and applied
	store       map[string]any            // the state machine: key -> value
	nextIndex   map[string]int            // leader only: the next index to send to each peer
	matchIndex  map[string]int            // leader only: how much of each peer's log is known to match ours
	waiting     map[int]quorumlab.Message // leader only: index -> the client request to answer when it commits
}

func main() {
	n := quorumlab.NewNode()
	r := &raft{
		n:          n,
		role:       "follower",
		log:        []Entry{},
		store:      map[string]any{},
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
		waiting:    map[int]quorumlab.Message{},
	}

	n.Handle("request_vote", r.handleRequestVote)
	n.Handle("append_entries", r.handleAppendEntries)
	n.Handle("put", r.handlePut)
	n.Handle("get", r.handleGet)
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
	// No longer leader: whoever is waiting on us should ask someone else.
	for _, client := range r.waiting {
		r.n.Reply(client, map[string]any{"type": "not_leader", "leader": nil})
	}
	clear(r.waiting)
	r.resetElectionTimer()
}

func (r *raft) startElection(ctx context.Context) {
	r.term++
	r.role = "candidate"
	r.votedFor = r.n.ID()
	r.leader = ""
	r.votes = 1
	r.resetElectionTimer()

	// TODO: tell the voter how good this log is: its length and the term of its last entry.
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
	for _, peer := range r.n.Peers() {
		r.nextIndex[peer] = len(r.log) + 1
		r.matchIndex[peer] = 0 // nothing is known until the peer says so
	}
	r.replicate(ctx)
}

func (r *raft) replicate(ctx context.Context) {
	for _, peer := range r.n.Peers() {
		r.sendEntries(ctx, peer)
	}
}

func (r *raft) sendEntries(ctx context.Context, peer string) {
	sentTerm := r.term
	prevIndex := r.nextIndex[peer] - 1
	// TODO: also send prevTerm, the term of the entry at prevIndex (0 if prevIndex is 0).
	req := appendEntries{
		Type:      "append_entries",
		Term:      r.term,
		PrevIndex: prevIndex,
		Entries:   append([]Entry{}, r.log[prevIndex:]...), // a copy: r.log changes once we unlock
		Commit:    r.commitIndex,
	}
	go func() {
		res, err := r.n.Call(ctx, peer, req, 100*time.Millisecond)
		if err != nil {
			return
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
			r.nextIndex[peer] = max(r.nextIndex[peer], reply.MatchIndex+1)
			r.matchIndex[peer] = max(r.matchIndex[peer], reply.MatchIndex)
			r.advanceCommit()
		} else {
			r.nextIndex[peer] = min(r.nextIndex[peer], reply.LogLength+1)
			r.sendEntries(ctx, peer)
		}
	}()
}

// advanceCommit (leader): the highest index stored on a majority of the
// whole cluster is committed.
func (r *raft) advanceCommit() {
	for n := len(r.log); n > r.commitIndex; n-- {
		copies := 1 // our own
		for _, peer := range r.n.Peers() {
			if r.matchIndex[peer] >= n {
				copies++
			}
		}
		if copies >= r.majority() {
			r.commitTo(n)
			break
		}
	}
}

// commitTo applies entries in order, up to index. This is the only place the
// state machine changes.
func (r *raft) commitTo(index int) {
	for r.commitIndex < index {
		entry := r.log[r.commitIndex]
		r.commitIndex++
		r.store[entry.Key] = entry.Value
		if client, ok := r.waiting[r.commitIndex]; ok {
			delete(r.waiting, r.commitIndex)
			r.n.Reply(client, map[string]any{"type": "put_ok"})
		}
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
	// TODO: refuse if our log is more up to date than the candidate's.
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

	// TODO: having an entry at prevIndex is not enough: its term must be prevTerm.
	// And an entry of ours that differs from the leader's at the same index
	// must go, together with everything after it.
	if len(r.log) < req.PrevIndex {
		return r.refuseEntries(msg)
	}
	for i, entry := range req.Entries {
		if index := req.PrevIndex + 1 + i; index > len(r.log) {
			r.log = append(r.log, entry)
		}
	}
	// The leader's commit index may be ahead of what this message covered.
	matched := req.PrevIndex + len(req.Entries)
	r.commitTo(min(req.Commit, matched))
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": matched})
}

// refuseEntries answers an append_entries that is stale or would leave a
// hole, with our term and how long our log is.
func (r *raft) refuseEntries(msg quorumlab.Message) error {
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": false, "logLength": len(r.log)})
}

func (r *raft) handlePut(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != "leader" {
		return r.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(r.leader)})
	}
	r.log = append(r.log, Entry{Term: r.term, Key: req.Key, Value: req.Value})
	r.waiting[len(r.log)] = msg // answered in commitTo, not here
	r.replicate(ctx)
	return nil
}

func (r *raft) handleGet(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Key string `json:"key"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "get_ok", "value": r.store[req.Key]})
}

func (r *raft) handleLog(ctx context.Context, msg quorumlab.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "log_ok", "entries": r.log, "commit": r.commitIndex})
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
