Real batch jobs are rarely one MapReduce. They form a **workflow**: the output of one job is the input of the next, often dozens of stages long. Between stages, the output is written out in full, or **materialized**. That costs storage and time, but a crashed stage can restart from the files of the stage before it, and the next stage starts only when its input is complete.

## What you build

The MapReduce from the previous task, now running a job made of stages:

```json
→ { "type": "job", "inputs": ["in-0", "in-1", "in-2", "in-3", "in-4", "in-5"],
    "stages": [ { "fn": "wordcount", "reduces": 3, "output": "wc" },
                { "fn": "bycount",   "reduces": 2, "output": "bc" } ] }
← { "type": "job_ok", "outputs": ["bc-0", "bc-1"] }
```

Each stage is a full MapReduce: a map phase and then a reduce phase. Stage `s` writes `<output>-<r>` for each reduce partition `r`. Stage 0 reads the job's `inputs`. Stage `s + 1` has one map task per output of stage `s` and reads them as its splits (`wc-0`, `wc-1`, `wc-2`). Start a stage only when the stage before it is done.

`bycount` reads `"word total"` lines. Its map emits `[total, word]`, and its reduce writes `"total word1 word2 ..."` with the words sorted. Keys are partitioned with `hash(key) % R` as before, and output lines are sorted by key compared as strings, so `"10"` comes before `"2"`.

Use the stage in intermediate file names, e.g. `mr-<stage>-<m>-<r>`. Every rule of the earlier tasks still holds: atomic commits, re-execution of a silent worker's task, backup tasks.

## The idea

A workflow is a chain of jobs joined by materialized files. Atomic commits make the boundary clean: the next stage sees each input either complete or not at all.

## The tests check

- `job_ok` within **3000ms**, listing the last stage's outputs
- every `wc-*` and `bc-*` file exactly right, and never half-written
- every fs request succeeds (reading a stage-one output that does not exist yet is an error)
- no task runs more than **3** times; n1 sends no fs requests; message budget as before
