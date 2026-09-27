// Package checker judges a parsed history against the guarantees in
// DESIGN.md. It never touches the network: evidence in, verdict out.
package checker

import (
	"fmt"
	"slices"

	"github.com/omkar619-dev/driftlab/internal/history"
)

// Verdict is a run's overall outcome.
type Verdict string

const (
	Clean   Verdict = "clean"
	Failed  Verdict = "failed"
	Invalid Verdict = "invalid"
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

// Result is everything the checker concluded about one history. Reasons
// says why a run is invalid; Unknown counts publishes that ended as info.
type Result struct {
	Verdict   Verdict
	Anomalies []Anomaly
	Reasons   []string
	Publishes int
	Unknown   int
}

// Check judges h: invalid if the run can't count as evidence, failed if it
// broke a promise, clean otherwise. The ordered-consumer contract only
// holds for a contiguous stream, which the final read must show (DESIGN.md
// principle 5).
func Check(h *history.History) Result {
	r := Result{Anomalies: checkOrdered(h.Ops)}
	r.Reasons = append(checkFinalRead(h.Ops), checkFaults(h)...)
	for _, op := range h.Ops {
		if op.F != history.Publish {
			continue
		}
		switch op.Type {
		case history.Invoke:
			r.Publishes++
		case history.Info:
			r.Unknown++
		}
	}
	switch {
	case len(r.Reasons) > 0:
		r.Verdict = Invalid
	case len(r.Anomalies) > 0:
		r.Verdict = Failed
	default:
		r.Verdict = Clean
	}
	return r
}

func checkFinalRead(ops []history.Op) []string {
	seqs := map[uint64]bool{}
	var last uint64
	for _, op := range ops {
		if op.F == history.Read {
			seqs[op.StreamSeq] = true
			last = max(last, op.StreamSeq)
		}
	}
	if len(seqs) == 0 {
		return []string{"no final read, so the stream's contents are unknown"}
	}
	missing := last - uint64(len(seqs))
	if missing == 0 {
		return nil
	}
	first := uint64(1)
	for seqs[first] {
		first++
	}
	return []string{fmt.Sprintf("final read is not contiguous: %d of %d stream sequences missing, starting at %d", missing, last, first)}
}

func checkFaults(h *history.History) []string {
	var reasons []string
	for _, fault := range h.Meta.Faults {
		fired := slices.ContainsFunc(h.Ops, func(op history.Op) bool {
			return op.Process == history.Nemesis && op.F == fault && op.Type == history.OK
		})
		if !fired {
			reasons = append(reasons, fmt.Sprintf("declared fault %q never completed", fault))
		}
	}
	return reasons
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
