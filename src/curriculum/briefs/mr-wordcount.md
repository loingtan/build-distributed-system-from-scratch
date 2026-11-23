Some jobs are too big for one machine: count every word in a few terabytes of logs. **MapReduce** splits such a job into many small tasks that any machine can run. Each **map task** reads one piece of the input (an **input split**) and turns it into key-value pairs. Each **reduce task** collects every pair for a range of keys and combines them. A **coordinator** keeps track of the tasks; **workers** run them.

In this track `n1` is the coordinator and every other node is a worker. The job is always word count, and the files live in a harness service called `fs`.

## What you build

A client sends the job to n1 and waits for the answer:

```json
→ { "type": "job", "inputs": ["in-0", "in-1", "in-2", "in-3", "in-4", "in-5"], "reduces": 3 }
← { "type": "job_ok", "outputs": ["out-0", "out-1", "out-2"] }
```

The fs service answers `node.rpc("fs", ...)`:

```json
→ { "type": "read", "name": "in-0" }             ← { "type": "read_ok", "data": ["the lines", "of the file"] }
→ { "type": "write", "name": "mr-0-1", "data": [...] }   ← { "type": "write_ok" }
→ { "type": "append", "name": "mr-0-1", "data": [...] }  ← { "type": "append_ok" }
→ { "type": "rename", "from": "a", "to": "b" }   ← { "type": "rename_ok" }
```

A write or append carries **at most 25 lines**; input files are read-only. Errors come back as `{ "type": "error", "code", "text" }`.

The starter gives you the job itself: `hash` (FNV-1a), `runMap` (read split `m`, write the pairs as `"word 1"` lines into `mr-<m>-<r>`, where `r = hash(word) % R`) and `runReduce` (read `mr-*-<r>`, add up, write `out-<r>`: one `"word total"` line per word, sorted by word). It runs them all on n1. Move that work to the workers; the node-to-node protocol is yours.

## The idea

The coordinator does only bookkeeping. The **shuffle** is the hash partitioning: every pair for a word lands in the same reduce partition, so reduce can start only after every map task is done.

## The tests check

- `job_ok` within **2500ms** of the job, with the outputs listed in order
- every `out-<r>` exactly right when `job_ok` is sent, and still right at the end
- n1 sends **no** fs requests; at least two workers run map tasks
- every fs request succeeds; no task runs more than **3** times
- at most **150** node-to-node messages per second on average (poll every 100ms or so)
