A deploy changes the code on a machine; a release changes what users get. Feature flags separate the two: ship the new code switched off, turn it on for one node (a **canary**), and switch it off again in milliseconds if it misbehaves.

## What you build

Version 2 can serve gets from the platform team's new read cache, behind the flag `cache_reads`, off by default. The harness rolls version 2 out as before (drain, stop, set `node.disk.version = 2`, restart); with the flag off, nothing may change. Then it sends

```json
→ { "type": "set_flag", "flag": "cache_reads", "on": true }
← { "type": "set_flag_ok" }
```

to one node, and later perhaps `"on": false` (the kill switch), or `true` to every node.

While its flag is on, a node answers **every** get by asking the cache first, with a 100ms timeout:

```json
→ cache { "type": "read", "key": "u7" }
← { "type": "read_ok", "name": "Ada Lovelace" }
← { "type": "error", "code": "unavailable" }
```

The cache always has the latest names, but it is new and fails now and then. When it fails, answer from the normal path: the client must not notice.

Each node also guards itself. It keeps the outcomes of its last 20 cache reads, and as soon as 5 of them are errors it turns `cache_reads` off by itself. Turning the flag on starts a fresh count. Flags live on `node.disk`, including one the node turned off itself, so a restart keeps them.

## The tests check

- nothing reads the cache until its flag is on, and `set_flag` takes effect at once, both ways
- with the flag on, every get asks the cache first, also after a restart
- a healthy cache (one error in seven reads) never trips the guard
- when the cache fails every other read, each node turns the flag off after the 5th error in 20, and it stays off after a restart
- not one client request fails, and gets return the latest name
