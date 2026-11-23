Often a node must pass on what *others* said: "three nodes voted blue". A traitor could make that up. A **signature** fixes this: a statement signed by n2 can be checked by anyone, whoever forwards it.

The starter gives you a toy scheme. `sign(text)` signs with your own key; `verify(signer, text, sig)` checks a signature. (Real systems use public-key signatures; the traitor only uses its own key.)

## What you build

Each node gets one input and sends a vote for it, signed over `"vote:" + value`, to every node (the starter does this):

```json
→ { "type": "input", "value": "blue" }
← { "type": "input_ok" }
   { "type": "vote", "value": "blue", "sig": "3fa1c2d0" }
```

- Count a vote only if `verify(msg.src, "vote:" + value, sig)` holds; only the first valid vote per node counts.
- When **2f+1 = 3** distinct nodes voted for one value, **decide** it. The three votes, as `{ "voter", "value", "sig" }`, are your **quorum certificate**.
- Then send `{ "type": "decided", "value": "blue", "cert": [ ... ] }` to your peers. A receiver decides too if the certificate holds at least 3 distinct known voters whose entries carry that value and verify.

```json
→ { "type": "read" }
← { "type": "read_ok", "value": "blue", "cert": [ { "voter": "n1", "value": "blue", "sig": "…" }, … ] }
```

`value` and `cert` are `null` until you decide.

## Why 3f+1

You can never wait for more than n - f votes, because f nodes may stay silent. Two such groups share n - 2f nodes, up to f of which may lie, so safety needs n - 2f ≥ f + 1: **n ≥ 3f + 1**. With n = 4, n - f = 2f + 1 = 3. The last test runs **three** nodes with one traitor. Keep the threshold at 3: no node can decide, and that is correct. A threshold of n - f = 2 would let two nodes decide different values.

## The tests check

- every decision carries a valid certificate, and honest nodes agree
- a decided value was some honest node's input
- with n = 4, every honest node decides within about 1 s when 2f+1 matching votes exist; with n = 3, none decides
