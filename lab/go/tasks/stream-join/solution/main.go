// A stream-table join in event time. Topic "customers" is a changelog:
//
//	{ offset, kind: "change", customer, tier, ts }   strictly increasing ts
//	{ offset, kind: "watermark", ts }                 no later change has ts <= this
//
// Topic "orders" holds { offset, order, customer, amount, ts }.
//
// Each order is joined with the customer's tier as it was at the order's
// ts. That needs every version of every customer (not just the latest), and
// patience: an order waits until the customers log has passed its ts,
// because the changelog lags behind the orders.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"time"

	"quorumlab"
)

type version struct {
	TS   int64
	Tier string
}

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
	versions := map[string][]version{} // customer -> versions, oldest first
	tableTime := int64(math.MinInt64)  // every change with ts <= tableTime has been read
	var waiting []order                // orders read but not yet joinable
	customersOffset, ordersOffset := 0, 0

	tierAt := func(customer string, ts int64) any {
		var tier any // null when the customer has no row yet
		for _, v := range versions[customer] {
			if v.TS <= ts {
				tier = v.Tier
			}
		}
		return tier
	}

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
						versions[r.Customer] = append(versions[r.Customer], version{r.TS, r.Tier})
					}
					// Changes come in ts order, so a change at ts also means
					// nothing earlier is still to come; a watermark promises the same.
					tableTime = max(tableTime, r.TS)
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
					waiting = append(waiting, o)
				}
			}
			// Join every order the table has caught up with.
			var still []order
			for _, o := range waiting {
				if o.TS > tableTime {
					still = append(still, o)
					continue
				}
				emit(ctx, map[string]any{"type": "enriched", "order": o.Order, "customer": o.Customer, "amount": o.Amount, "tier": tierAt(o.Customer, o.TS)})
			}
			waiting = still
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
