// Exactly-once results from at-least-once parts. Topic "payments" holds
// { offset, account, amount }; for each payment the node sends the sink
//
//	{ type: "emit", id, offset, account, balance }
//
// and the sink ignores an id it has already stored.
//
// Two rules make every payment count exactly once:
//  1. The checkpoint (offset + balances) is one value on disk, written in
//     one fsynced Set, and only after the sink has acknowledged every output
//     it covers. A crash rewinds to a consistent point and replays.
//  2. A replayed payment is sent with the same id as the first time (the id
//     comes from the payment's offset), so the sink drops the repeat.
package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"time"

	"quorumlab"
)

type payment struct {
	Offset  int    `json:"offset"`
	Account string `json:"account"`
	Amount  int64  `json:"amount"`
}

type checkpoint struct {
	Offset   int              `json:"offset"`
	Balances map[string]int64 `json:"balances"`
}

func main() {
	n := quorumlab.NewNode()

	// Only the polling timer touches cp; Every never overlaps itself.
	cp := checkpoint{Balances: map[string]int64{}}

	emit := func(ctx context.Context, body map[string]any) {
		// Retry until acknowledged: the sink may be down or slow.
		for {
			res, err := n.Call(ctx, "sink", body, 400*time.Millisecond)
			if err == nil && res.Type() == "emit_ok" {
				return
			}
		}
	}

	n.OnStart(func(ctx context.Context) {
		if _, err := n.Disk().Get("checkpoint", &cp); err != nil {
			log.Fatalf("reading the checkpoint: %v", err)
		}
		if cp.Balances == nil {
			cp.Balances = map[string]int64{}
		}
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": "payments", "offset": cp.Offset, "max": 20}, 200*time.Millisecond)
			if err != nil || res.Type() != "poll_ok" {
				return
			}
			var body struct {
				Records []payment `json:"records"`
			}
			if res.Decode(&body) != nil {
				return
			}
			next := checkpoint{Offset: cp.Offset, Balances: maps.Clone(cp.Balances)}
			for _, p := range body.Records {
				if p.Offset != next.Offset {
					continue
				}
				next.Balances[p.Account] += p.Amount
				emit(ctx, map[string]any{"type": "emit", "id": fmt.Sprintf("payment-%d", p.Offset), "offset": p.Offset, "account": p.Account, "balance": next.Balances[p.Account]})
				next.Offset = p.Offset + 1
			}
			if next.Offset == cp.Offset {
				return
			}
			// Every output up to next.Offset is acknowledged: commit state
			// and offset together.
			if err := n.Disk().Set("checkpoint", next); err != nil {
				n.Log("saving the checkpoint: %v", err)
				return
			}
			cp = next
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
