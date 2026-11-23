An LSM tree never changes data in place. The **B-tree**, inside almost every relational database, does the opposite: the data lives in fixed-size **pages** that are updated where they are. Pages form a tree. Inner pages hold separator keys and pointers to children; leaves hold the keys and values. Every leaf is at the same depth, so a lookup reads one page per level.

When a write would overfill a page it **splits** in two and the parent gains a separator key. If the root splits, a new root is made above it: the tree grows at the top, which is why it stays balanced.

## What you build

`put`, `get`, `delete` and `scan` as in the LSM task (`scan` returns `entries: [[key, value], ...]` for `from <= key < to`, in order). The tree is kept in `node.disk`:

```json
{
  "root": "7",
  "pages": {
    "7": { "leaf": false, "keys": ["k020"], "children": ["5", "6"] },
    "5": { "leaf": true, "keys": ["k003", "k011"], "values": ["v1", "v9"] },
    "6": { "leaf": true, "keys": ["k020", "k031"], "values": ["v4", "v2"] }
  }
}
```

A page holds at most **4 keys**, strictly ascending. An inner page has one more child than keys; child `i` holds keys from `keys[i-1]` (inclusive) to `keys[i]` (exclusive). Deletes may leave pages under-full or empty; no merging is needed. Other fields on `node.disk` are allowed.

## Crash safety

A split changes several pages. If power fails halfway, the tree is corrupt. Real B-trees prevent this with a **write-ahead log** of page changes, or with **copy-on-write**: write new copies of the changed pages, then switch `root` in one step, then free the old ones. Use either; after every request, the pages must be a valid tree holding every acknowledged write.

> In this lab a single handler's (or a single `Disk().Update`'s) changes reach the disk atomically, so a crash cannot tear a multi-page write in half, and the tests cannot catch a B-tree that updates pages in place. Real disks can tear writes, which is why databases use a write-ahead log or copy-on-write pages; build it that way here too.

## The tests check

- reads and scans return the latest data, also after crashes
- after every request: valid structure, at most 4 keys per page, all leaves at one depth, every page reachable from the root, and the tree holds every acknowledged write
- the tree reaches at least three levels
- a write adds, changes or removes at most 3 × height + 2 pages
