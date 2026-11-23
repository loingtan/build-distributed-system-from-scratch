// Exactly-once output. Read the brief: npm run lab -- brief stream-exactly-once
//
// Topic "payments" holds { offset, account, amount }; for each payment the
// node sends the sink
//
//	{ type: "emit", id, offset, account, balance }
//
// and the sink ignores an id it has already stored.
//
// This version keeps the offset and the balances in memory and makes up a
// fresh id for every output. After a crash it starts again from offset 0,
// which the log has deleted by then.
//
// TODO: save the offset and the balances on disk, together (one Set of one
// value, or one Update), and only once the outputs they cover have been
// acknowledged. Give each output an id that a replay of the same payment
// will reproduce.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"quorumlab"
)

type payment struct {
	Offset  int    `json:"offset"`
	Account string `json:"account"`
	Amount  int64  `json:"amount"`
}

func main() {
	n := quorumlab.NewNode()

	// Only the polling timer touches this state; Every never overlaps itself.
	offset := 0
	balances := map[string]int64{}

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
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": "payments", "offset": offset, "max": 20}, 200*time.Millisecond)
			if err != nil || res.Type() != "poll_ok" {
				return
			}
			var body struct {
				Records []payment `json:"records"`
			}
			if res.Decode(&body) != nil {
				return
			}
			for _, p := range body.Records {
				if p.Offset != offset {
					continue
				}
				balances[p.Account] += p.Amount
				offset = p.Offset + 1
				emit(ctx, map[string]any{"type": "emit", "id": fmt.Sprintf("%s-%d", n.ID(), int64(n.Rand()*1e9)), "offset": p.Offset, "account": p.Account, "balance": balances[p.Account]})
			}
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
