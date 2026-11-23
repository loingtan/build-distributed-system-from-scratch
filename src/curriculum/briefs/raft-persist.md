Every guarantee so far rests on promises. A follower that said "I have entry 12" let the leader commit it. A node that voted in term 5 promised not to vote again in term 5. If a crash erases the promise, the guarantee goes with it: the entry can vanish from the majority, or one term can get two leaders.

## What you build

The same store and client messages: `put` → `put_ok` or `not_leader`, `get` → `get_ok`, `log` → `log_ok { entries, commit }`, and `status`. Now nodes crash and restart. A restart runs your file from the top. Variables are gone; only `node.disk` remains:

```js
node.disk.log ??= [];          // empty on first start, the old log after a restart
let term = node.disk.term ?? 0;
```

## The idea

Raft names exactly three things that must be durable:

- the **current term**
- **who this node voted for** in that term
- the **log**

Write them to `node.disk` whenever they change, **before** sending the message that depends on them. Everything else (role, commit index, the store, the leader's `nextIndex` and `matchIndex`) is rebuilt: a restarted node starts as a follower with `commit` at 0 and an empty store, and applies its log again as the leader tells it what is committed.

After a full-cluster restart, a new leader holds entries from an older term. By the rule from the last task, they commit when the first entry of its own term does.

## Timing

While a majority is running, writes are acknowledged within **2s**. From **2.5s** after the last crashed node returns, every put must get `put_ok` within 2s. A node is probed right after it restarts. A crash can land the instant a node has sent a message, so anything written to disk after sending may be lost.

## The tests check

- the leader crashes and returns as a follower under a newer leader
- all nodes crash at once and restart together
- rolling crashes of one, two, then all five nodes
- a node votes and crashes the instant its answer is sent; restarted, it is asked again in the same term (clients are held off for 2.8s while this is staged)
- throughout: at most one leader per term, a node's term never goes backwards, and no acknowledged put is ever lost
