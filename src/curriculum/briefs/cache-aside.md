Databases are slow compared with memory, and most traffic is lopsided: a few products, profiles or settings are asked for far more often than the rest. Reading the same row a thousand times a second is wasted work. A **cache** keeps recent answers in memory so that only the first request pays for the trip to the database.

In this track your nodes sit in front of a database, a service called `db`. Clients ask your nodes; your nodes ask the database.

## What you build

A client asks for a key:

```json
→ { "type": "get", "key": "k7" }
← { "type": "get_ok", "value": "k7:v0:4821" }
```

The database answers reads in **30–60ms**, through `node.rpc('db', ...)`:

```json
→ { "type": "read", "key": "k7" }
← { "type": "read_ok", "value": "k7:v0:4821" }
```

Pass the database's value on unchanged. In this task values never change, so an entry you have read stays correct forever.

## The idea

This pattern is called **cache-aside**: the application looks in the cache first, and on a **miss** reads the database and stores the answer itself. The database knows nothing about the cache. The share of requests answered from memory is the **hit rate**, and with skewed traffic even a small cache gets a high one.

Each node keeps its own cache. Two nodes that both see key `k1` will each read it once.

## The tests check

- every get gets a reply, carrying the value the database holds
- the database receives at most **1.5 reads per distinct (node, key) pair, plus 5**. The slack covers a few requests for the same key that arrive together before the first read has returned; the stampede task ([CA-4](#/c/cache-stampede)) deals with those properly
- with 300 gets over 30 keys on one node, and 400 gets over 40 keys on two nodes; a few keys get most of the traffic
