# Quorum Lab: the local runner (Go)

Every node is a real OS process. Run them as plain processes on your machine, or
one Docker container per node. The tests, fault schedules and checkers are the
same ones the browser lab uses; what changes is that processes, time, crashes and
disks are real.

```sh
npm run lab -- list                    # every task, with your progress
npm run lab -- brief broadcast-faults  # read a brief in the terminal
npm run lab -- new broadcast-faults    # creates practice/broadcast-faults (Go)
npm run lab -- test broadcast-faults   # build and run all its tests
npm run lab -- test broadcast-faults --docker          # one container per node
npm run lab -- test broadcast-faults --test 2 --verbose
npm run lab -- hints broadcast-faults
```

Edit `practice/<task>/main.go`, run `test`, repeat. Results are also saved to
`.lab/results/<task>.json`.

## What is real and what is not

| | Local runner |
|---|---|
| Processes | One OS process per node; `--docker` puts each in its own container (own filesystem, PID space, 512 MB memory limit, no network) |
| Concurrency | Real: Go handlers run in parallel goroutines |
| Crashes | `SIGKILL` of the node's process group. Memory, goroutines and timers are gone |
| Pauses | `SIGSTOP`, then `SIGCONT` after the pause, of the node's process group. Memory is kept; messages wait in its stdin and timers fire late |
| Disk | `disk.json` in the node's data directory, written atomically and fsynced by `Disk.Set` / `Disk.Update` |
| Time | Real wall-clock time. Tests take as long as they say (3–14 s each) |
| Network | Messages travel through the runner, which adds latency (2–12 ms by default), loss, partitions and per-message delays exactly as the tests specify |
| Determinism | The fault schedule is seeded (`--seed`), but process scheduling is not, so runs differ slightly |

The network is simulated on purpose: it is what lets a test stage an exact
sequence (Raft's Figure 8, a delayed message overtaken by newer ones) and draw the
message timeline. Maelstrom, the tool behind fly.io's Gossip Glomers, works the
same way.

## The protocol

Each node reads JSON lines on stdin and writes JSON lines on stdout
(Maelstrom-style framing); stderr is its log. The first message is
`{"type":"init","node_id":"n1","node_ids":[...],"data_dir":"...","epoch_ms":...,"kv":false}`
and must be answered with `init_ok`. The Go library does all of this for you.
Any language that speaks the protocol works with `npm run lab -- test <task> <dir>`
once a runner for it exists; for now the CLI builds Go directories and
browser-lab `.js` files.

## The Go library (`lab/go/quorumlab`)

Briefs show the browser lab's JavaScript API. The Go equivalents:

| Browser (`node.…`) | Go (`n := quorumlab.NewNode()`) |
|---|---|
| `node.on('type', (msg) => …)` | `n.Handle("type", func(ctx context.Context, msg quorumlab.Message) error {…})` |
| `msg.body.field` | `msg.Decode(&req)` into a struct or `map[string]any`; `msg.Type()` |
| `node.reply(msg, body)` | `n.Reply(msg, body)` |
| `node.send(dest, body)` | `n.Send(ctx, dest, body)` |
| `await node.rpc(dest, body, { timeout })` → body or `null` | `reply, err := n.Call(ctx, dest, body, timeout)`; `err == quorumlab.ErrTimeout` when nothing came back |
| `node.after(ms, fn)` / `node.every(ms, fn)` → `{ cancel() }` | `n.After(d, func(ctx context.Context){…})` / `n.Every(…)` → `*Timer` with `Stop()` |
| top-level code that starts timers | `n.OnStart(func(ctx context.Context){…})` (runs again after a restart) |
| `node.id`, `node.peers`, `node.nodeIds` | `n.ID()`, `n.Peers()`, `n.NodeIDs()` |
| `node.now()` | `n.Elapsed()` (since the test started); `n.Now()` is the node's wall clock |
| `node.random()` | `n.Rand()` |
| `node.log(…)` | `n.Log(format, args…)` (stderr; never print to stdout) |
| `node.disk.key = value` | `n.Disk().Set("key", value)` (returns after fsync); `n.Disk().Get("key", &v)`; `n.Disk().Update(func(tx *quorumlab.Tx) error {…})` for several keys atomically |
| `await node.kv.read/write/cas` | `n.KV().Read(ctx, key, &v)`, `Write(ctx, key, v)`, `CAS(ctx, key, from, to)` |

Things that differ from the browser lab, because Go is concurrent and the disk is real:

- **Handlers run concurrently.** Two messages can be handled at the same time.
  Guard shared state with a `sync.Mutex`, and don't hold it across `n.Call`.
- **Disk writes are explicit.** In the browser, assigning to `node.disk` is
  durable at once. Here nothing is durable until `Disk.Set`/`Update` returns. Write
  before you reply, as a real server must.
- **Pass the handler's `ctx` to `Send` and `Call`.** It tags your messages with the
  event that caused them, which a few checkers use to tell one handler run from
  another.
- **The disk layout is visible.** Each key you `Set` becomes a top-level field of
  `disk.json`. Where a brief says what the disk must contain (for example
  `node.disk.log`), use that key name (`Set("log", entries)`).
