// Event-time windows. Read the brief: npm run lab -- brief stream-windows
//
// Topic "clicks" holds { offset, kind: "event", key, ts } and
// { offset, kind: "watermark", ts }.
//
// The polling loop is done: it hands every record to handle(), once and in
// log order.
package main

import (
	"context"
	"log"
	"math"
	"sync/atomic"
	"time"

	"quorumlab"
)

const (
	window   = 1000
	lateness = 500
)

type record struct {
	Offset int    `json:"offset"`
	Kind   string `json:"kind"`
	Key    string `json:"key"`
	TS     int64  `json:"ts"`
}

func main() {
	n := quorumlab.NewNode()

	// Only the polling timer touches this state (Every never overlaps
	// itself); dropped is also read by the stats handler.
	offset := 0
	watermark := int64(math.MinInt64)
	var dropped atomic.Int64

	emit := func(ctx context.Context, body map[string]any) {
		// The sink is reliable, but ask again if a reply goes missing.
		for {
			if _, err := n.Call(ctx, "sink", body, 300*time.Millisecond); err == nil {
				return
			}
		}
	}

	handle := func(ctx context.Context, r record) {
		// TODO: an event belongs to the window [start, start+window) that
		// holds its ts (careful: round down, also for negative numbers).
		// Count it there, unless that window has already closed: then count
		// it in dropped instead.
		//
		// TODO: a watermark record moves watermark forward. Every window
		// whose end + lateness is now <= watermark closes: send one
		//   { type: "window", key, start, end, count }
		// per key with events in it, then forget the window.
		_, _, _ = watermark, emit, r
	}

	n.OnStart(func(ctx context.Context) {
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": "clicks", "offset": offset, "max": 20}, 200*time.Millisecond)
			if err != nil {
				return
			}
			var body struct {
				Records []record `json:"records"`
			}
			if res.Type() != "poll_ok" || res.Decode(&body) != nil {
				return
			}
			for _, r := range body.Records {
				if r.Offset != offset {
					continue
				}
				offset++
				handle(ctx, r)
			}
		})
	})

	n.Handle("stats", func(ctx context.Context, msg quorumlab.Message) error {
		return n.Reply(msg, map[string]any{"type": "stats_ok", "dropped": dropped.Load()})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
