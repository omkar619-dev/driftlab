// Package checker judges a parsed history against the guarantees in
// DESIGN.md. It never touches the network: evidence in, anomalies out.
package checker

import (
	"fmt"

	"github.com/omkar619-dev/driftlab/internal/history"
)

// Kind names an anomaly. The names come from Jepsen's Kafka workload.
type Kind string

const (
	PollSkip         Kind = "poll-skip"
	NonmonotonicPoll Kind = "nonmonotonic-poll"
	Duplicate        Kind = "duplicate"
)

// Anomaly is one broken promise, with the history indexes that prove it.
type Anomaly struct {
	Kind    Kind
	Process string
	Ops     []int
	Msg     string
}

// Check judges h against the ordered-consumer contract: each consumer must
// receive stream sequences 1, 2, 3, ... with no gaps, repeats or
// reordering. That contract only holds for a contiguous stream (DESIGN.md
// principle 5).
func Check(h *history.History) []Anomaly {
	return checkOrdered(h.Ops)
}

type consumer struct {
	max   uint64         // highest stream_seq delivered so far
	maxOp int            // history index of that delivery; -1 before the first
	seen  map[uint64]int // stream_seq -> history index of its first delivery
}

type skip struct {
	at      int    // index in the anomaly list
	process string // the consumer that skipped
	lo, hi  uint64 // the skipped stream sequences, inclusive
}

func checkOrdered(ops []history.Op) []Anomaly {
	var out []Anomaly
	var skips []skip
	consumers := map[string]*consumer{}
	for _, op := range ops {
		if op.F != history.Deliver {
			continue
		}
		c := consumers[op.Process]
		if c == nil {
			c = &consumer{maxOp: -1, seen: map[uint64]int{}}
			consumers[op.Process] = c
		}
		s := op.StreamSeq
		if first, ok := c.seen[s]; ok {
			out = append(out, Anomaly{
				Kind:    Duplicate,
				Process: op.Process,
				Ops:     []int{first, op.Index},
				Msg:     fmt.Sprintf("stream_seq %d delivered again", s),
			})
			continue
		}
		c.seen[s] = op.Index
		switch {
		case s < c.max:
			out = append(out, Anomaly{
				Kind:    NonmonotonicPoll,
				Process: op.Process,
				Ops:     []int{c.maxOp, op.Index},
				Msg:     fmt.Sprintf("stream_seq %d delivered after %d", s, c.max),
			})
			continue
		case s > c.max+1:
			ev := []int{op.Index}
			if c.maxOp >= 0 {
				ev = []int{c.maxOp, op.Index}
			}
			skips = append(skips, skip{at: len(out), process: op.Process, lo: c.max + 1, hi: s - 1})
			out = append(out, Anomaly{Kind: PollSkip, Process: op.Process, Ops: ev})
		}
		c.max, c.maxOp = s, op.Index
	}
	for _, sk := range skips {
		seen := consumers[sk.process].seen
		var late uint64
		for q := sk.lo; q <= sk.hi; q++ {
			if _, ok := seen[q]; ok {
				late++
			}
		}
		n := sk.hi - sk.lo + 1
		out[sk.at].Msg = fmt.Sprintf("expected stream_seq %d, got %d: %d skipped, %d delivered late, %d never delivered",
			sk.lo, sk.hi+1, n, late, n-late)
	}
	return out
}
