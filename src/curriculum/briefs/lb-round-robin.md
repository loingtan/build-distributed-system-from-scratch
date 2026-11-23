One server can only do so much. To serve more, you run several identical copies, called **backends**, and put a **load balancer** in front of them. Clients talk to the balancer only; it passes each request to one backend and passes the answer back. Clients never learn which backend did the work.

In this track your node `n1` is that load balancer.

## What you build

First the harness tells you which backends make up the pool. It can send this again at any time with a different list:

```json
→ { "type": "set_backends", "backends": ["b1", "b2", "b3"] }
← { "type": "set_backends_ok" }
```

Then clients send requests:

```json
→ { "type": "get", "key": "r17" }
```

Forward each one to a backend with `node.rpc("b2", { type: "get", key })`. The backend answers:

```json
← { "type": "get_ok", "key": "r17", "value": "r17@b2#s123", "backend": "b2" }
```

Pass that answer back to the client unchanged: `node.reply(msg, res)`.

## The idea: round robin

Give the backends turns in order: b1, b2, b3, b1, b2, … It needs no knowledge about the backends at all, and over any stretch of requests every backend gets the same number, give or take one.

Many requests are in flight at once and backends answer in any order. Because each handler awaits its own `node.rpc`, every answer goes back to the client that asked for it.

## The tests check

- `set_backends` is acknowledged
- every request gets a reply within **1000ms**, and the reply is the backend's answer for that key
- each request goes to **exactly one** backend
- after `set_backends_ok`, only backends in the new list get requests
- while the pool is unchanged, the backends' request counts differ by **at most 2**
- this holds with three backends, with five backends of very different speeds receiving bursts of 10 requests, and when the pool grows from three to four and shrinks to two
