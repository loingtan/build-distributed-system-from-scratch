A write is on W replicas, but the reader does not know which. Ask a single replica and it may be one that missed the write. The fix is to read from several, and to be able to tell which answer is newest.

## What you build

Two additions to the previous task. Keys are now overwritten, and clients can read:

```json
→ { "type": "get", "key": "k1" }
← { "type": "get_ok", "value": "apple" }
```

`value` is `null` for a key that was never written. The coordinator asks the replicas, waits for **R** answers and returns the newest. Choose W and R so that **W + R > N**; with the N = 3 and N = 5 clusters in the tests, a majority for both does it (for an even N, R = N − W + 1 is the smallest that works). If R replicas do not answer, reply `{ "type": "error", "text": "..." }` within **800ms**, as for `put`.

`put` and `local_get` keep their shape from the previous task.

## The idea

If W + R > N, any W replicas and any R replicas have at least one member in common. So among the R answers there is always one from a replica that stored the last acknowledged write. The read only has to recognise it.

For that, every stored value carries a **version**: a counter, with the coordinator's node id to break ties. A replica replaces its copy only if the incoming version is higher, and a read returns the value with the highest version.

The trap is choosing the counter. A coordinator that was cut off for a while holds an old counter. If it numbers a new write from its own copy, that write is acknowledged and then loses to an older one. Before writing, ask R replicas which version they hold and go one higher.

## The tests check

- every request is answered, with `put_ok`, `get_ok` or `error`
- a `get` never returns a value older than the last `put` on that key acknowledged before the `get` began
- gets and puts succeed while the coordinator can reach a majority
- a coordinator that can reach only a minority refuses both
- right after a partition heals, the nodes that were cut off coordinate reads and writes correctly
- with no faults, every replica ends on the same, newest version, even when two puts to one key race through different nodes
