Your program runs as **one real OS process per node**: plain processes on this machine, or one Docker container each. Every node runs the same `main.go`. Nodes share nothing; they talk only through messages the lab runner delivers, adding latency, loss, partitions and crashes.

```go
func main() {
	n := quorumlab.NewNode()
	n.Handle("echo", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Echo string `json:"echo"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "echo_ok", "echo": req.Echo})
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
```

## Identity

| | |
|---|---|
| `n.ID()` | This node's id, e.g. `"n1"` |
| `n.Peers()` | Ids of the other nodes |
| `n.NodeIDs()` | Ids of all nodes, this one included |

## Receiving

| | |
|---|---|
| `n.Handle("type", func(ctx, msg) error {…})` | Handle one message type. Returning an error reports it as a failure of this node |
| `msg.Src` | Who sent it: a node id, or a client such as `"c1"` |
| `msg.Decode(&req)` | Unmarshal the body into a struct or `map[string]any` |
| `msg.Type()` | The body's `type` field |

**Handlers run concurrently**, each in its own goroutine. Guard shared state with a `sync.Mutex`, and never hold it across `n.Call`.

## Sending

| | |
|---|---|
| `n.Reply(msg, body)` | Answer `msg` (adds `in_reply_to`). A reply received from another node can be relayed as is |
| `n.Send(ctx, dest, body)` | Fire and forget. May be lost; you will not be told |
| `reply, err := n.Call(ctx, dest, body, timeout)` | Send and wait. `err == quorumlab.ErrTimeout` if nothing came back in time |

Pass the handler's `ctx` to `Send` and `Call`: it tags your messages with the event that caused them, which some checkers use. Bodies are structs or maps that marshal to a JSON object.

## Time

| | |
|---|---|
| `n.OnStart(func(ctx) {…})` | Runs once the node knows its id and peers, and again after every restart. Start timers here |
| `n.After(d, func(ctx) {…})` | Run once, later. Returns a `*Timer` with `Stop()` |
| `n.Every(d, func(ctx) {…})` | Run repeatedly |
| `n.Elapsed()` | Time since the test started (the checker's clock) |
| `n.Now()` | This node's wall clock |
| `n.Rand()` | A random float in [0, 1) |

## Logging

`n.Log(format, args…)` writes to stderr, shown in the **Logs** tab. Never print to stdout: it carries protocol messages only.

## Disk (survives a crash)

| | |
|---|---|
| `n.Disk().Get("key", &v)` | Read a value; reports `false` if nothing is stored |
| `n.Disk().Set("key", v)` | Store and **fsync** before returning |
| `n.Disk().Update(func(tx *quorumlab.Tx) error {…})` | Change several keys in one atomic write |

A crash is a real `SIGKILL`: memory, goroutines and timers are gone, and the program starts again from `main`. Nothing is durable until `Set` or `Update` returns, so write before you reply. Where a brief names a disk key (`node.disk.log`), use that name: some checkers read `disk.json`.

## Shared store (only in challenges that mention it)

| | |
|---|---|
| `n.KV().Read(ctx, key, &v)` | Reports `false` if the key was never written |
| `n.KV().Write(ctx, key, v)` | |
| `n.KV().CAS(ctx, key, from, to)` | `true` if it held `from` and now holds `to`; `from` nil means "only if missing" |

## The network

- Messages take a few milliseconds and can overtake one another. During a **partition**, messages between the two sides are dropped; clients can always reach every node.
- Tests take real time (3–14 s each), and process scheduling is real, so runs differ slightly. If a test fails once, run it again before debugging.
