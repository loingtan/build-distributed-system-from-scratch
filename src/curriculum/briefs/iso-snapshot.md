Read committed still lets a transaction see the world change under it. Alice has two accounts of 500. A transfer moves 100 between them and commits while her balance report is halfway through: the report read the first account before the transfer and the second after, and shows 900. Nothing dirty was read; the two reads simply came from different moments. That is **read skew**, and reading the same key twice and getting two answers is a **non-repeatable read**.

## What you build

The same protocol and rules as the previous task, now with **snapshot isolation**: each transaction reads from one consistent snapshot of the database, **taken at `begin`**. Commits that land after its `begin` stay invisible to it, however long it runs; its own writes are still visible to it.

The usual implementation is **multi-version concurrency control (MVCC)**: keep several versions of each key, each stamped with the commit that wrote it, and let each transaction pick the versions its snapshot allows. Readers never block writers, and writers never block readers.

Versions pile up, so also answer a new message:

```json
→ { "type": "versions" }
← { "type": "versions_ok", "count": 6 }
```

`count` is the total number of versions you still store, across all keys. A version may be dropped once no open transaction can read it. The tests ask after every transaction has finished, when `count` must not exceed the number of keys ever written.

In these tests writers blindly rewrite whole groups of keys; nothing forces an abort, and up to half of the attempts may be aborted.

## The tests check

- everything from the previous task
- reads are repeatable within a transaction
- **no read skew**: every writer rewrites all three keys of a group, so a transaction that reads a group sees one writer's values for all of them
- a transaction never sees a commit that was requested after its `begin` was answered
- one transaction stays open for 700ms while commits pile up, and still sees its snapshot
- old versions are garbage-collected
