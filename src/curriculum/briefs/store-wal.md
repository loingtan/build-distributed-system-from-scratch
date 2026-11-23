A database that says "saved" has made a promise: the write is still there after the power goes out. Memory does not keep that promise. Only the disk does, and writing a whole data structure to disk on every change is slow and can be cut off halfway.

The standard answer is a **write-ahead log**: before changing anything, append a short record of the change to a file. Appending is cheap and either happens or does not. After a crash, read the log from the start and apply every record again, and you are back where you were.

## What you build

A key-value store on one node. Keys and values are strings.

```json
→ { "type": "put", "key": "k003", "value": "v17" }
← { "type": "put_ok" }

→ { "type": "delete", "key": "k003" }
← { "type": "delete_ok" }

→ { "type": "get", "key": "k003" }
← { "type": "get_ok", "value": null }
```

A missing or deleted key reads as `null`. Keep the data in memory for reads, as the starter does, and add the log.

## The disk

`node.disk` is a plain object that survives a crash; everything else is lost, and the file runs again from the top. Keep the log as an **array at `node.disk.log`**. What each entry looks like is up to you.

The rule that makes it work: **log first, acknowledge last.** If the reply goes out before the record is on disk, a crash in between loses a write the client was told is safe.

## The tests check

- every `put`, and every `delete` of a key that exists, makes `node.disk.log` longer before its reply arrives
- gets return the latest write
- the node is crashed and restarted (sometimes twice in a row) right after a write is acknowledged, and every acknowledged write and delete is still there

There is one client, which sends one request at a time and waits up to 500ms for each reply.
