// Sortable IDs: pack time, node number and sequence into one integer.
package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"

	"quorumlab"
)

// One integer, three fields, most significant first:
//
//	[ time in ms ][ node number: 6 bits ][ sequence: 12 bits ]
//
// Time leads, so ids sort by when they were made. The node number keeps two
// nodes apart within the same millisecond; the sequence keeps one node's ids
// apart within the same millisecond.
//
// Go's int is 64 bits, so shifting is safe here (the JavaScript version has
// to multiply instead). The result stays far below 2^53, so it survives JSON.
const (
	nodeBits = 6
	seqBits  = 12
	maxSeq   = 1<<seqBits - 1
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	nodeNumber := int64(0)
	lastMs := int64(-1)
	seq := int64(0)

	// The node's id is known once the node starts: "n3" -> 3.
	n.OnStart(func(ctx context.Context) {
		num, err := strconv.Atoi(strings.TrimPrefix(n.ID(), "n"))
		if err != nil {
			n.Log("cannot read a node number from %q: %v", n.ID(), err)
		}
		mu.Lock()
		nodeNumber = int64(num)
		mu.Unlock()
	})

	// next returns the next id. The caller holds mu.
	next := func() int64 {
		ms := n.Elapsed().Milliseconds()
		if ms <= lastMs {
			// Same millisecond as the last id, or the clock went backwards:
			// never go back, take the next sequence number instead.
			ms = lastMs
			seq++
			if seq > maxSeq {
				// Out of sequence numbers for this millisecond: borrow the next one.
				ms++
				seq = 0
			}
		} else {
			seq = 0
		}
		lastMs = ms
		return (ms<<nodeBits|nodeNumber)<<seqBits | seq
	}

	n.Handle("generate", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		id := next()
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": id})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
