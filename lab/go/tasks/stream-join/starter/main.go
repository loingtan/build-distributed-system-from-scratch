// Stream-table join. Read the brief: npm run lab -- brief stream-join
//
// Topic "customers" is a changelog:
//
//	{ offset, kind: "change", customer, tier, ts }   strictly increasing ts
//	{ offset, kind: "watermark", ts }                 no later change has ts <= this
//
// Topic "orders" holds { offset, order, customer, amount, ts }.
//
// This version joins each order with the table as it is at the moment the
// order is read (processing time). The customers log lags behind the
// orders, so an order often sees a tier that is too old, and an order read
// late sees one that is too new.
//
// TODO: join each order with the tier its customer had at the order's ts.
// Keep every version of each customer, and hold an order back until the
// customers log has passed its ts.
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"quorumlab"
)

type custRecord struct {
	Offset   int    `json:"offset"`
	Kind     string `json:"kind"`
	Customer string `json:"customer"`
	Tier     string `json:"tier"`
	TS       int64  `json:"ts"`
}

type order struct {
	Offset   int             `json:"offset"`
	Order    string          `json:"order"`
	Customer string          `json:"customer"`
	Amount   json.RawMessage `json:"amount"`
	TS       int64           `json:"ts"`
}

func main() {
	n := quorumlab.NewNode()

	// Only the polling timer touches this state; Every never overlaps itself.
	table := map[string]string{} // customer -> current tier
	customersOffset, ordersOffset := 0, 0

	read := func(ctx context.Context, topic string, offset int, out any) bool {
		res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": topic, "offset": offset, "max": 20}, 200*time.Millisecond)
		if err != nil || res.Type() != "poll_ok" {
			return false
		}
		return res.Decode(out) == nil
	}

	emit := func(ctx context.Context, body map[string]any) {
		for {
			if _, err := n.Call(ctx, "sink", body, 300*time.Millisecond); err == nil {
				return
			}
		}
	}

	n.OnStart(func(ctx context.Context) {
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			var cs struct {
				Records []custRecord `json:"records"`
			}
			if read(ctx, "customers", customersOffset, &cs) {
				for _, r := range cs.Records {
					if r.Offset != customersOffset {
						continue
					}
					customersOffset++
					if r.Kind == "change" {
						table[r.Customer] = r.Tier
					}
				}
			}
			var os struct {
				Records []order `json:"records"`
			}
			if read(ctx, "orders", ordersOffset, &os) {
				for _, o := range os.Records {
					if o.Offset != ordersOffset {
						continue
					}
					ordersOffset++
					var tier any
					if t, ok := table[o.Customer]; ok {
						tier = t
					}
					emit(ctx, map[string]any{"type": "enriched", "order": o.Order, "customer": o.Customer, "amount": o.Amount, "tier": tier})
				}
			}
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
