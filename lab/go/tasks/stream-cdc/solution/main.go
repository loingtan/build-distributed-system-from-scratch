// Change data capture: a derived view kept up to date by following the
// database's changelog, topic "users" on the log service. Each record is
//
//	{ offset, key, before, after }   (before null: insert, after null: delete)
//
// The view (rows, counts) and the offset of the next change to apply are
// saved together in one atomic disk write, so a crash never separates them:
// after a restart the node resumes exactly where its saved view ends.
package main

import (
	"context"
	"log"
	"maps"
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
		mu.Lock()
		defer mu.Unlock()
		for key, v := range map[string]any{"offset": &offset, "rows": &rows, "counts": &counts} {
			if _, err := n.Disk().Get(key, v); err != nil {
				log.Fatalf("reading %s from disk: %v", key, err)
			}
		}
		// Every runs one poll at a time: a slow reply never overlaps the next poll.
		n.Every(25*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			from := offset
			mu.Unlock()
			res, err := n.Call(ctx, "log", map[string]any{"type": "poll", "topic": "users", "offset": from, "max": 20}, 200*time.Millisecond)
			if err != nil {
				return // lost or slow: ask again on the next tick
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
			// Apply to copies, save them with the new offset, then show them.
			nextRows, nextCounts, next := maps.Clone(rows), maps.Clone(counts), offset
			for _, c := range body.Records {
				if c.Offset != next {
					continue // strictly in order, each change once
				}
				if c.Before != nil {
					nextCounts[*c.Before]--
					if nextCounts[*c.Before] == 0 {
						delete(nextCounts, *c.Before)
					}
				}
				if c.After != nil {
					nextCounts[*c.After]++
					nextRows[c.Key] = *c.After
				} else {
					delete(nextRows, c.Key)
				}
				next++
			}
			if next == offset {
				return
			}
			err = n.Disk().Update(func(tx *quorumlab.Tx) error {
				if err := tx.Set("offset", next); err != nil {
					return err
				}
				if err := tx.Set("rows", nextRows); err != nil {
					return err
				}
				return tx.Set("counts", nextCounts)
			})
			if err != nil {
				n.Log("saving the view: %v", err)
				return
			}
			offset, rows, counts = next, nextRows, nextCounts
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
