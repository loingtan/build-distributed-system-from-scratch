// Event-time tumbling windows with a watermark and allowed lateness.
// Topic "clicks" holds { offset, kind: "event", key, ts } and
// { offset, kind: "watermark", ts }.
//
// Windows come from each event's own ts, never from the clock. A window
// [start, start+1000) stays open until the watermark reaches end + 500;
// events that arrive before that still count, even if they are behind the
// watermark. Once closed, its counts go to the sink and the window is
// forgotten; anything that turns up for it later is dropped.
package main

import (
	"context"
	"log"
	"math"
	"sort"
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

func floorTo(ts int64) int64 {
	return int64(math.Floor(float64(ts)/window)) * window
}

func main() {
	n := quorumlab.NewNode()

	// Only the polling timer touches this state (Every never overlaps
	// itself); dropped is also read by the stats handler.
	offset := 0
	watermark := int64(math.MinInt64)
	var dropped atomic.Int64
	open := map[int64]map[string]int{} // window start -> key -> count

	emit := func(ctx context.Context, body map[string]any) {
		// The sink is reliable, but ask again if a reply goes missing.
		for {
			if _, err := n.Call(ctx, "sink", body, 300*time.Millisecond); err == nil {
				return
			}
		}
	}

	handle := func(ctx context.Context, r record) {
		switch r.Kind {
		case "event":
			start := floorTo(r.TS)
			if watermark != math.MinInt64 && watermark >= start+window+lateness {
				dropped.Add(1) // its window has already been emitted
				return
			}
			if open[start] == nil {
				open[start] = map[string]int{}
			}
			open[start][r.Key]++
		case "watermark":
			watermark = max(watermark, r.TS)
			var closing []int64
			for start := range open {
				if start+window+lateness <= watermark {
					closing = append(closing, start)
				}
			}
			sort.Slice(closing, func(i, j int) bool { return closing[i] < closing[j] })
			for _, start := range closing {
				counts := open[start]
				delete(open, start)
				for key, count := range counts {
					emit(ctx, map[string]any{"type": "window", "key": key, "start": start, "end": start + window, "count": count})
				}
			}
		}
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
