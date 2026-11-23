// Raft log repair: log matching with prevTerm, removal of conflicting
// entries, the election restriction, and committing only entries of the
// leader's own term (Figure 8).
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

type requestVote struct {
	Type         string `json:"type"`
	Term         int    `json:"term"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type appendEntries struct {
	Type      string  `json:"type"`
	Term      int     `json:"term"`
	PrevIndex int     `json:"prevIndex"`
	PrevTerm  int     `json:"prevTerm"`
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

// termAt is the term of entry i, or 0 for i == 0.
func (r *raft) termAt(i int) int {
	if i == 0 {
		return 0
	}
	return r.log[i-1].Term
}

func (r *raft) lastLogTerm() int { return r.termAt(len(r.log)) }

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
	wasLeader := r.role == "leader"
	r.term = term
	r.role = "follower"
	r.votedFor = ""
	r.leader = ""
	// No longer leader: whoever is waiting on us should ask someone else.
	for _, client := range r.waiting {
		r.n.Reply(client, map[string]any{"type": "not_leader", "leader": nil})
	}
	clear(r.waiting)
	// Hearing of a newer term is not hearing from a leader: keep the election
	// timer running (a deposed leader starts one). Otherwise candidates whose
	// logs are too stale to win keep resetting it, and the node that could win
	// never stands.
	if wasLeader {
		r.resetElectionTimer()
	}
}

func (r *raft) startElection(ctx context.Context) {
	r.term++
	r.role = "candidate"
	r.votedFor = r.n.ID()
	r.leader = ""
	r.votes = 1
	r.resetElectionTimer()

	ask := requestVote{Type: "request_vote", Term: r.term, LastLogIndex: len(r.log), LastLogTerm: r.lastLogTerm()}
	for _, peer := range r.n.Peers() {
		go r.requestVote(ctx, peer, ask)
	}
}

func (r *raft) requestVote(ctx context.Context, peer string, ask requestVote) {
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
	if r.role != "candidate" || r.term != ask.Term || !vote.Granted {
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
	req := appendEntries{
		Type:      "append_entries",
		Term:      r.term,
		PrevIndex: prevIndex,
		PrevTerm:  r.termAt(prevIndex),
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
			// The follower's log differs from ours at or before prevIndex.
			// Step back one entry (or straight to the end of its log) and try again.
			r.nextIndex[peer] = max(1, min(r.nextIndex[peer]-1, reply.LogLength+1))
			r.sendEntries(ctx, peer)
		}
	}()
}

// advanceCommit (leader): the highest index stored on a majority of the
// whole cluster is committed, but only if that entry is from our own term.
// An older entry on a majority can still be overwritten; it commits along
// with a newer one.
func (r *raft) advanceCommit() {
	for n := len(r.log); n > r.commitIndex; n-- {
		if r.termAt(n) != r.term {
			break
		}
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
	var req requestVote
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	// The election restriction: never vote for a log that is behind ours.
	// A later last term wins; with equal last terms the longer log wins.
	upToDate := req.LastLogTerm > r.lastLogTerm() || (req.LastLogTerm == r.lastLogTerm() && req.LastLogIndex >= len(r.log))
	granted := req.Term == r.term && upToDate && (r.votedFor == "" || r.votedFor == msg.Src)
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

	// Log matching: accept the entries only if our log is identical to the
	// leader's up to prevIndex. Same index and same term means same history.
	if req.PrevIndex > len(r.log) || r.termAt(req.PrevIndex) != req.PrevTerm {
		return r.refuseEntries(msg)
	}
	for i, entry := range req.Entries {
		index := req.PrevIndex + 1 + i
		// An entry of ours that conflicts with the leader's was never
		// committed. Drop it and all after it. Matching entries stay, even
		// when this message is a late duplicate.
		if index <= len(r.log) && r.log[index-1].Term != entry.Term {
			r.log = r.log[:index-1]
		}
		if index > len(r.log) {
			r.log = append(r.log, entry)
		}
	}
	// The leader's commit index may be ahead of what this message covered.
	matched := req.PrevIndex + len(req.Entries)
	r.commitTo(min(req.Commit, matched))
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": matched})
}

// refuseEntries answers an append_entries that is stale or does not attach
// to our log, with our term and how long our log is.
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
