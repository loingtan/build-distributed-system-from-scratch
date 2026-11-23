So far the database never changed. Real data does, and often not through you: another service writes to the same table, an admin script fixes a row, a nightly job recomputes prices. Your cache is not told. Unless entries expire, a cached value can be wrong forever.

## What you build

The LRU cache from the last task, with a **time to live** (TTL) of **1000ms**. An entry may be served for up to 1000ms after it was read; after that it counts as a miss and is read again.

```json
→ { "type": "get", "key": "k3" }
← { "type": "get_ok", "value": "k3:v4:4880" }
```

Here the harness changes the database directly, behind your nodes' backs. Each key gets a new version every few hundred milliseconds.

## The idea

A TTL does not make the cache correct. It puts a **bound on staleness**: no answer is ever more than 1000ms out of date. You trade freshness for load. A shorter TTL means fresher answers and more database reads; a longer one means fewer reads and older answers. Many systems can live with "at most a second old", and a TTL is the simplest way to promise it.

Date each entry from when you **sent** the read: the value is at least that fresh, and the time the answer spent travelling then counts against the TTL instead of being added to it. Do not refresh that date on a hit, or a popular key never expires.

## The tests check

- every get gets a value the database really held for that key
- no get returns a value that the database had replaced more than **1100ms** before the get was sent (the 1000ms TTL plus 100ms for the network)
- at most **40%** of gets reach the database; a 1000ms TTL needs about 20%, so expiring much earlier than 1000ms is too often
- on one node with 10 keys changing about every 700ms, and on two nodes with 12 keys changing about every 400ms
