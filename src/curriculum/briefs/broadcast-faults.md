So far the network has delivered every message. Real networks do not. Links drop packets, and sometimes a whole group of nodes is cut off from the rest: a **partition**.

Your flooding solution sends each value once and hopes. Run it against the partition test and open the Timeline: the red messages are the ones that never arrived.

## What you build

The same broadcast system, with one stronger promise: every acknowledged value reaches every node **eventually**, whatever the network loses along the way.

## The tools

`node.send` cannot tell you whether a message arrived. `node.rpc` can:

```js
const ack = await node.rpc(peer, { type: 'gossip', message: 7 }, { timeout: 300 });
// ack is the reply body, or null if nothing came back in 300ms
```

The receiver answers with `node.reply(msg, { type: 'gossip_ok' })`.

## Things to think about

- A missing acknowledgement means the request was lost **or** the reply was lost. You cannot tell which. What should the sender do, and what must the receiver tolerate as a result?
- A node must still acknowledge the client immediately. It cannot wait for a partition to heal.

## The tests check

- a healthy network (flooding passes this one)
- a two-second partition, then recovery
- a network that loses a third of all messages between nodes
