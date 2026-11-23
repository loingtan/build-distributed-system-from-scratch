A distributed system is a set of **nodes** that share nothing except the messages they send each other. Before building anything clever, get one node to answer a message.

## What you build

A node that answers every `echo` message with the same payload.

```json
→ { "type": "echo", "echo": "hello 1", "msg_id": 1 }
← { "type": "echo_ok", "echo": "hello 1", "in_reply_to": 1 }
```

You write the handler; `node.reply` fills in `in_reply_to` so the client can match the answer to its question.

## How the lab works

- The code in the editor runs **once on every node**. Each node gets its own copy of every variable.
- Simulated clients send requests. A checker then decides whether your cluster behaved.
- Press **Run tests** (or ⌘/Ctrl + Enter). The **Timeline** tab shows every message that crossed the network.

## The tests check

- every request gets a reply
- the reply has type `echo_ok`
- the payload comes back unchanged
