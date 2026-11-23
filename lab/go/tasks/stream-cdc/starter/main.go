// Change data capture. Read the brief: npm run lab -- brief stream-cdc
//
// A derived view kept up to date by following the database's changelog,
// topic "users" on the log service. Each record is
//
//	{ offset, key, before, after }   (before null: insert, after null: delete)
//
// This version keeps everything in memory. It works until the node
// crashes: after a restart it starts again from offset 0, and the log no
// longer has those changes.
//
// TODO: keep the view and the offset on disk (n.Disk().Update writes several
// keys atomically), and change them together so that a crash never leaves
// one ahead of the other. Read them back in OnStart.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type change struct {
	Offset int     `json:"offset"`
	Key    string  `json:"key"`
	Before *string `json:"before"`
	After  *string `json:"after"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	offset := 0                 // the next change to apply = how many have been applied
	rows := map[string]string{} // user -> city, a copy of the table
	counts := map[string]int{}  // city -> number of users there

	n.OnStart(func(ctx context.Context) {
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			from := offset
			mu.Unlock()
			res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": "users", "offset": from, "max": 20}, 200*time.Millisecond)
			if err != nil {
				return
			}
			var body struct {
				Records []change `json:"records"`
			}
			if res.Type() != "poll_ok" || res.Decode(&body) != nil {
				n.Log("poll failed: %s", res.Body)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, c := range body.Records {
				if c.Before != nil {
					counts[*c.Before]--
					if counts[*c.Before] == 0 {
						delete(counts, *c.Before)
					}
				}
				if c.After != nil {
					counts[*c.After]++
					rows[c.Key] = *c.After
				} else {
					delete(rows, c.Key)
				}
				offset = c.Offset + 1
			}
		})
	})

	n.Handle("view", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "view_ok", "offset": offset, "rows": rows, "counts": counts})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
