// Package idgen produces time-sortable 53-bit ids.
//
// Layout (high to low): 41 bits milliseconds since EpochMs | 4 bits node | 8 bits sequence.
// 53 bits total, so an id is exact as a float64 (Redis sorted-set score) and as a
// JavaScript number, and ids sort by creation time. One node can mint 256 ids per
// millisecond; up to 16 nodes may run at once (the API assigns node numbers through Redis,
// node 15 is reserved for the seeder).
package idgen

import (
	"sync"
	"time"
)

const (
	// EpochMs is 2026-01-01T00:00:00Z.
	EpochMs  int64 = 1767225600000
	nodeBits       = 4
	seqBits        = 8
	MaxNode        = 1<<nodeBits - 1
	maxSeq         = 1<<seqBits - 1
)

// Make builds an id from its parts. It is also used by the seeder to backdate data.
func Make(ms int64, node, seq int) int64 {
	return (ms-EpochMs)<<(nodeBits+seqBits) | int64(node)<<seqBits | int64(seq)
}

// Time returns the creation time embedded in an id.
func Time(id int64) time.Time {
	return time.UnixMilli(EpochMs + id>>(nodeBits+seqBits))
}

// MinIDAt returns the smallest id that could have been minted at t.
func MinIDAt(t time.Time) int64 { return Make(t.UnixMilli(), 0, 0) }

// Gen mints ids for one node. It is safe for concurrent use.
type Gen struct {
	mu     sync.Mutex
	node   int
	lastMs int64
	seq    int
	now    func() int64
}

func New(node int) *Gen {
	return &Gen{node: node & MaxNode, now: func() int64 { return time.Now().UnixMilli() }}
}

// Next returns a new id. If the clock goes backwards, or more than 256 ids are
// requested within one millisecond, it keeps counting on a logical clock instead of
// issuing a duplicate.
func (g *Gen) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := g.now()
	if ms <= g.lastMs {
		ms = g.lastMs
		g.seq++
		if g.seq > maxSeq {
			ms++
			g.seq = 0
		}
	} else {
		g.seq = 0
	}
	g.lastMs = ms
	return Make(ms, g.node, g.seq)
}
