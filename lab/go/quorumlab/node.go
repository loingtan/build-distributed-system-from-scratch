// Package quorumlab is the Go library for Quorum Lab nodes.
//
// A node is one OS process. It talks to the rest of the cluster only through
// JSON messages on stdin/stdout (Maelstrom-compatible framing); the lab
// runner delivers them, adding latency, loss, partitions and crashes.
//
//	func main() {
//		n := quorumlab.NewNode()
//		n.Handle("echo", func(ctx context.Context, msg quorumlab.Message) error {
//			var body map[string]any
//			if err := msg.Decode(&body); err != nil {
//				return err
//			}
//			body["type"] = "echo_ok"
//			return n.Reply(msg, body)
//		})
//		if err := n.Run(); err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Handlers run concurrently, each in its own goroutine, as in a real server:
// protect shared state with a mutex. Call blocks until the reply arrives or
// the timeout passes, while other messages keep being handled.
package quorumlab

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTimeout is returned by Call when no reply arrives in time.
var ErrTimeout = errors.New("quorumlab: no reply before the timeout")

// Message is one message received from the network.
type Message struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
	// Event is the runner's id for the delivery of this message. Replies and
	// sends made while handling it are tagged with it, so the checker can tell
	// which messages one handler run produced. You do not need to touch it.
	Event int64 `json:"id,omitempty"`
}

type header struct {
	Type      string `json:"type"`
	MsgID     *int64 `json:"msg_id,omitempty"`
	InReplyTo *int64 `json:"in_reply_to,omitempty"`
	Code      *int   `json:"code,omitempty"`
	Text      string `json:"text,omitempty"`
}

func (m Message) header() header {
	var h header
	_ = json.Unmarshal(m.Body, &h)
	return h
}

// Type is the body's "type" field.
func (m Message) Type() string { return m.header().Type }

// Decode unmarshals the body into v (a struct or a map).
func (m Message) Decode(v any) error { return json.Unmarshal(m.Body, v) }

// IsError reports whether the body is an error reply ({"type":"error",...}).
func (m Message) IsError() bool { return m.Type() == "error" }

// Handler handles one message type. Returning an error reports it to the
// lab as a failure of this node.
type Handler func(ctx context.Context, msg Message) error

type eventKey struct{}

func eventOf(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx.Value(eventKey{}).(int64); ok {
		return v
	}
	return 0
}

func withEvent(ctx context.Context, id int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, eventKey{}, id)
}

// Node is one member of the cluster.
type Node struct {
	mu       sync.RWMutex
	id       string
	nodeIDs  []string
	dataDir  string
	epoch    time.Time
	offset   time.Duration
	hasKV    bool
	handlers map[string]Handler
	onStart  []func(ctx context.Context)

	pendingMu sync.Mutex
	pending   map[int64]chan Message

	nextMsgID  atomic.Int64
	timerEvent atomic.Int64

	outMu sync.Mutex
	out   *bufio.Writer
	in    io.Reader

	warnedMu sync.Mutex
	warned   map[string]bool

	disk *Disk
	kv   *KV
}

// NewNode creates a node. Register handlers, then call Run.
func NewNode() *Node {
	n := &Node{
		handlers: map[string]Handler{},
		pending:  map[int64]chan Message{},
		warned:   map[string]bool{},
		out:      bufio.NewWriterSize(os.Stdout, 1<<16),
		in:       os.Stdin,
	}
	// Message ids must not repeat after a restart, or a late reply to the old
	// process could be taken for a reply to the new one.
	n.nextMsgID.Store(time.Now().UnixMicro() % (1 << 50))
	n.kv = &KV{n: n}
	return n
}

// Handle registers the handler for a message type. A second call for the
// same type replaces the first.
func (n *Node) Handle(typ string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[typ] = h
}

// OnStart registers code to run once the node knows its id and peers, before
// it receives any message: start timers here. It runs again after a restart.
func (n *Node) OnStart(f func(ctx context.Context)) { n.onStart = append(n.onStart, f) }

// ID is this node's id, e.g. "n1".
func (n *Node) ID() string { return n.id }

// NodeIDs lists every node in the cluster, this one included.
func (n *Node) NodeIDs() []string { return append([]string(nil), n.nodeIDs...) }

// Peers lists every other node.
func (n *Node) Peers() []string {
	out := make([]string, 0, len(n.nodeIDs))
	for _, id := range n.nodeIDs {
		if id != n.id {
			out = append(out, id)
		}
	}
	return out
}

// Elapsed is the time since the test started, the same clock the checker
// reports times on. (This is node.now() in the browser lab.)
func (n *Node) Elapsed() time.Duration { return time.Since(n.epoch) + n.offset }

// Now is this node's wall clock. In tests with clock skew it disagrees with
// other nodes' clocks by up to the stated amount.
func (n *Node) Now() time.Time { return time.Now().Add(n.offset) }

// Rand returns a random float in [0, 1).
func (n *Node) Rand() float64 { return rand.Float64() }

// Disk is this node's durable storage. It survives crashes; memory does not.
func (n *Node) Disk() *Disk { return n.disk }

// KV is the shared linearizable store, in challenges that provide one.
func (n *Node) KV() *KV { return n.kv }

// Log writes a line to the node's log (stderr). Never print to stdout:
// stdout carries protocol messages only.
func (n *Node) Log(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func (n *Node) reportError(where string, err any) {
	line, _ := json.Marshal(map[string]string{"where": where, "message": fmt.Sprint(err)})
	fmt.Fprintf(os.Stderr, "!error %s\n", line)
}

// withFields adds protocol fields (msg_id, in_reply_to) to a body. Fields
// named in drop are removed first.
func withFields(body any, extra map[string]any, drop ...string) (json.RawMessage, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("message body is not JSON: %w", err)
	}
	if len(extra) == 0 && len(drop) == 0 {
		return raw, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, fmt.Errorf("a message body must be a JSON object (a struct or a map), got %s", truncate(raw))
	}
	for _, k := range drop {
		delete(m, k)
	}
	for k, v := range extra {
		b, _ := json.Marshal(v)
		m[k] = b
	}
	return json.Marshal(m)
}

func truncate(b []byte) string {
	if len(b) > 80 {
		return string(b[:77]) + "..."
	}
	return string(b)
}

type outEnvelope struct {
	Src   string          `json:"src"`
	Dest  string          `json:"dest"`
	Body  json.RawMessage `json:"body"`
	Cause int64           `json:"cause"`
}

func (n *Node) write(cause int64, dest string, body json.RawMessage) error {
	line, err := json.Marshal(outEnvelope{Src: n.id, Dest: dest, Body: body, Cause: cause})
	if err != nil {
		return err
	}
	n.outMu.Lock()
	defer n.outMu.Unlock()
	n.out.Write(line)
	n.out.WriteByte('\n')
	return n.out.Flush()
}

// Send sends a message and does not wait for an answer. It may be lost;
// you will not be told.
func (n *Node) Send(ctx context.Context, dest string, body any) error {
	raw, err := withFields(body, nil)
	if err != nil {
		return err
	}
	return n.write(eventOf(ctx), dest, raw)
}

// Reply answers req. It adds in_reply_to so the sender can match the answer.
// Any msg_id or in_reply_to already in body is replaced, so a reply received
// from another node can be relayed as it is: n.Reply(msg, res.Body).
func (n *Node) Reply(req Message, body any) error {
	h := req.header()
	extra := map[string]any{}
	if h.MsgID != nil {
		extra["in_reply_to"] = *h.MsgID
	}
	raw, err := withFields(body, extra, "msg_id", "in_reply_to")
	if err != nil {
		return err
	}
	return n.write(req.Event, req.Src, raw)
}

// Call sends a request and waits for the reply. It returns ErrTimeout if no
// reply arrives within timeout, and ctx's error if ctx is cancelled first.
func (n *Node) Call(ctx context.Context, dest string, body any, timeout time.Duration) (Message, error) {
	id := n.nextMsgID.Add(1)
	raw, err := withFields(body, map[string]any{"msg_id": id})
	if err != nil {
		return Message{}, err
	}
	ch := make(chan Message, 1)
	n.pendingMu.Lock()
	n.pending[id] = ch
	n.pendingMu.Unlock()
	defer func() {
		n.pendingMu.Lock()
		delete(n.pending, id)
		n.pendingMu.Unlock()
	}()
	if err := n.write(eventOf(ctx), dest, raw); err != nil {
		return Message{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case m := <-ch:
		return m, nil
	case <-t.C:
		return Message{}, ErrTimeout
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

// Timer is a scheduled callback. Stop cancels it.
type Timer struct {
	once sync.Once
	stop chan struct{}
}

// Stop cancels the timer. It is safe to call more than once.
func (t *Timer) Stop() {
	if t != nil {
		t.once.Do(func() { close(t.stop) })
	}
}

func (n *Node) timerCtx() context.Context {
	return withEvent(context.Background(), -n.timerEvent.Add(1))
}

func (n *Node) safely(where string, f func()) {
	defer func() {
		if r := recover(); r != nil {
			n.reportError(where, fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
		}
	}()
	f()
}

// After runs f once, after d.
func (n *Node) After(d time.Duration, f func(ctx context.Context)) *Timer {
	t := &Timer{stop: make(chan struct{})}
	go func() {
		tm := time.NewTimer(d)
		defer tm.Stop()
		select {
		case <-tm.C:
			n.safely("timer", func() { f(n.timerCtx()) })
		case <-t.stop:
		}
	}()
	return t
}

// Every runs f repeatedly, every d, until the timer is stopped. A slow f
// delays the next run rather than overlapping with it.
func (n *Node) Every(d time.Duration, f func(ctx context.Context)) *Timer {
	t := &Timer{stop: make(chan struct{})}
	go func() {
		tk := time.NewTicker(d)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				n.safely("timer", func() { f(n.timerCtx()) })
			case <-t.stop:
				return
			}
		}
	}()
	return t
}

type initBody struct {
	NodeID        string   `json:"node_id"`
	NodeIDs       []string `json:"node_ids"`
	DataDir       string   `json:"data_dir"`
	EpochMs       int64    `json:"epoch_ms"`
	ClockOffsetMs int64    `json:"clock_offset_ms"`
	KV            bool     `json:"kv"`
}

// Run reads messages until stdin closes. It returns only on a protocol
// error or when the lab shuts the node down.
func (n *Node) Run() error {
	sc := bufio.NewScanner(n.in)
	sc.Buffer(make([]byte, 1<<20), 64<<20)

	// The first message is always init.
	if !sc.Scan() {
		return sc.Err()
	}
	var first Message
	if err := json.Unmarshal(sc.Bytes(), &first); err != nil {
		return fmt.Errorf("quorumlab: bad init line: %w", err)
	}
	var ib initBody
	if err := first.Decode(&ib); err != nil || first.Type() != "init" {
		return fmt.Errorf("quorumlab: expected init, got %s", truncate(first.Body))
	}
	n.id, n.nodeIDs, n.dataDir, n.hasKV = ib.NodeID, ib.NodeIDs, ib.DataDir, ib.KV
	sort.Strings(n.nodeIDs)
	n.epoch = time.UnixMilli(ib.EpochMs)
	n.offset = time.Duration(ib.ClockOffsetMs) * time.Millisecond
	disk, err := openDisk(n.dataDir)
	if err != nil {
		return err
	}
	n.disk = disk
	for _, f := range n.onStart {
		n.safely("startup", func() { f(withEvent(context.Background(), first.Event)) })
	}
	if err := n.Reply(first, map[string]any{"type": "init_ok"}); err != nil {
		return err
	}

	for sc.Scan() {
		var msg Message
		if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
			n.Log("quorumlab: ignoring malformed line: %s", truncate(sc.Bytes()))
			continue
		}
		h := msg.header()
		if h.InReplyTo != nil {
			n.pendingMu.Lock()
			ch, ok := n.pending[*h.InReplyTo]
			delete(n.pending, *h.InReplyTo)
			n.pendingMu.Unlock()
			if ok {
				ch <- msg
				continue
			}
		}
		n.mu.RLock()
		handler := n.handlers[h.Type]
		n.mu.RUnlock()
		if handler == nil {
			if h.InReplyTo == nil {
				n.warnedMu.Lock()
				if !n.warned[h.Type] {
					n.warned[h.Type] = true
					n.Log("no handler for %q messages (from %s); ignoring them", h.Type, msg.Src)
				}
				n.warnedMu.Unlock()
			}
			continue
		}
		go n.safely(fmt.Sprintf("handler %q", h.Type), func() {
			if err := handler(withEvent(context.Background(), msg.Event), msg); err != nil {
				n.reportError(fmt.Sprintf("handler %q", h.Type), err)
			}
		})
	}
	return sc.Err()
}
