# Porting a task to Go

Each task has a JavaScript starter and solution (`src/curriculum/code/<slug>.{starter,solution}.js`)
used by the browser lab. The local runner needs Go versions:

```
lab/go/tasks/<slug>/starter/main.go
lab/go/tasks/<slug>/solution/main.go
```

All of `lab/go/tasks` is one Go module (`module tasks`, importing `quorumlab`).
Read first: `lab/README.md` (the Go API and how it maps to the JS API), the
library itself (`lab/go/quorumlab/*.go`), and the finished examples
`lab/go/tasks/{echo,broadcast-faults,g-counter}`.

## Rules

- **Same behaviour, same protocol.** The solution must speak exactly the client
  messages and fields the brief and the JS solution use. The node-to-node protocol
  may stay the same as the JS solution's (simplest) unless Go suggests something
  clearer.
- **The starter mirrors the JS starter**: the same amount of given code, the same
  TODO comments in the same places, the same deliberate bug or gap. It must fail
  at least one test (and should fail the same tests the JS starter fails). Begin it
  with `// <Title>. Read the brief: npm run lab -- brief <slug>` and keep the JS
  starter's explanatory comments.
- **Idiomatic Go.** `gofmt`, small helper functions, structs for request bodies
  (`var req struct { Key string \`json:"key"\` }`), `map[string]any` for replies is
  fine. One `sync.Mutex` guarding node state is usually right; never hold it across
  `n.Call`. Keep the JS solution's comments that explain the idea.
- **Concurrency is real.** Handlers run in parallel goroutines. Where the JS
  solution relies on "a handler runs to completion before the next message"
  (serial transactions, a log append, a state machine), take the mutex for the
  whole critical section.
- **Disk is explicit.** Translate `node.disk.x = …` into `n.Disk().Set("x", …)`
  (or `Update` for several keys at once) *before* the reply or message that
  depends on it, and `node.disk.x ?? default` at startup into `Disk().Get`. Use the
  same key names as the JS code: some checkers read `disk.json` and look for them.
- **Timers** start in `n.OnStart(...)`. Pass the handler's or timer's `ctx` to
  `Send`/`Call` (the vector-clock and feature-flag checkers group sends by it).
- **Go notes.** Where the brief or hints give JavaScript-only advice (e.g. "don't use `<<`",
  `Math.max`, "a handler runs to completion", `node.disk.x = …` being durable at once), write
  `lab/go/tasks/<slug>/GO.md`: two to four short bullets for a Go reader. The CLI prints it
  after the brief and after the hints. See `lab/go/tasks/sortable-ids/GO.md`.
- **Do not edit** anything outside `lab/go/tasks/<your slugs>/`. If the runner,
  library, a checker or a brief seems wrong, describe the problem in your report
  instead of changing it.

## Verify

```sh
npm run lab -- verify <slug> --kind both < /dev/null          # Go solution and starter, local processes
npm run lab -- verify <slug> --docker < /dev/null             # solution in Docker, one container per node
npm run lab -- verify --track <track-id> --kind both < /dev/null
```

Done when every Go solution passes every test (run each solution at least twice,
since real processes are not deterministic; a solution that fails once in a few
runs is not done), every starter fails at least one test, and `go vet ./...` in
`lab/go/tasks` is clean. Spot-check at least one task per track with `--docker`.

Real time means a track takes a few minutes to verify. Never run a bare `ls` or
any command that waits on stdin; append `< /dev/null` to node commands.
