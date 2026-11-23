Your code runs **once on every node**, top to bottom. Variables at the top of the file are that node's private state. Nodes share nothing; they can only exchange messages.

## Identity

| | |
|---|---|
| `node.id` | This node's id, e.g. `"n1"` |
| `node.peers` | Ids of the other nodes |
| `node.nodeIds` | Ids of all nodes, this one included |

## Receiving

```js
node.on('echo', (msg) => {
  msg.src    // who sent it: a node id, or a client such as "c1"
  msg.body   // the JSON payload; msg.body.type is 'echo'
});
```

One handler runs at a time on a node, and it runs to completion unless it `await`s. Registering a second handler for the same type replaces the first.

## Sending

| | |
|---|---|
| `node.reply(msg, body)` | Answer `msg`. Adds `in_reply_to` for you. |
| `node.send(dest, body)` | Fire and forget. May be lost; you will not be told. |
| `await node.rpc(dest, body, { timeout })` | Send and wait. Resolves with the reply body, or `null` if none arrives within `timeout` ms (default 1000). |

Bodies must be plain JSON: objects, arrays, strings, numbers, booleans, `null`. Convert a `Set` or `Map` first.

## Time

| | |
|---|---|
| `node.after(ms, fn)` | Run `fn` once, later. Returns `{ cancel() }`. |
| `node.every(ms, fn)` | Run `fn` repeatedly. Returns `{ cancel() }`. |
| `node.now()` | Simulated time in ms. Not synchronised between real machines, so do not lean on it. |
| `node.random()` | Like `Math.random()`, but repeatable from run to run. |

`setTimeout` and `setInterval` work too and use the same simulated clock.

## Debugging

`node.log(...)` or `console.log(...)` writes to the **Logs** tab, tagged with the node and the time.

## Shared store (only in challenges that mention it)

```js
await node.kv.read(key)            // value, or null if missing
await node.kv.write(key, value)
await node.kv.cas(key, from, to)   // true if it held `from` and now holds `to`
```

## Surviving a crash (only in challenges that crash nodes)

`node.disk` is a plain object that survives a crash; everything else (variables, timers, requests in flight) is lost. After a restart the file runs again from the top with the same `node.disk`.

```js
node.disk.log ??= [];        // first start: create; after a restart: already there
node.disk.log.push(entry);   // durable as soon as the line has run
```

Keep it plain JSON: a `Set` or `Map` on the disk does not survive.

## The network

- Each message takes a few milliseconds and messages can overtake one another.
- During a **partition**, messages between the two sides are dropped. Clients can always reach every node.
- Runs are deterministic: the same code gives the same run, so a failure can be replayed as often as you need.
