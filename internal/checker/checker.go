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

// Kind names an anomaly. Where Jepsen's Kafka workload has a name for one,
// we use it.
type Kind string

const (
	PollSkip         Kind = "poll-skip"
	NonmonotonicPoll Kind = "nonmonotonic-poll"
	Duplicate        Kind = "duplicate"
	Stall            Kind = "stall"
	LostWrite        Kind = "lost-write"
	Phantom          Kind = "phantom"
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

// Lines renders r the way the command line prints it: the verdict, then
// why the run is invalid, then each anomaly.
func (r Result) Lines() []string {
	out := []string{fmt.Sprintf("verdict %s, %d of %d publishes unknown", r.Verdict, r.Unknown, r.Publishes)}
	for _, reason := range r.Reasons {
		out = append(out, "invalid: "+reason)
	}
	for _, a := range r.Anomalies {
		out = append(out, fmt.Sprintf("%s %s ops=%v: %s", a.Kind, a.Process, a.Ops, a.Msg))
	}
	return out
}

// Check judges h: invalid if the run can't count as evidence, failed if it
// broke a promise, clean otherwise. The ordered-consumer contract only
// holds for a contiguous stream, so a hole in the final read that no acked
// publish explains makes the run invalid (DESIGN.md principle 5).
func Check(h *history.History) Result {
	final := readFinal(h.Ops)
	r := Result{Anomalies: checkOrdered(h.Ops)}
	if final != nil {
		r.Anomalies = append(r.Anomalies, checkStalls(h.Ops, final)...)
		r.Anomalies = append(r.Anomalies, checkLostWrites(h.Ops, final)...)
	}
	r.Anomalies = append(r.Anomalies, checkPhantoms(h.Ops)...)
	r.Reasons = append(checkFinalRead(h.Ops, final), checkFaults(h)...)
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

type finalRead struct {
	seqs   map[uint64]bool // stream sequences it returned
	values map[uint64]bool // values it returned
	last   uint64          // highest stream sequence it returned
	lastOp int             // history index of that record
	start  int             // history index of its first record
}

func readFinal(ops []history.Op) *finalRead {
	var f *finalRead
	for _, op := range ops {
		if op.F != history.Read {
			continue
		}
		if f == nil {
			f = &finalRead{seqs: map[uint64]bool{}, values: map[uint64]bool{}, start: op.Index}
		}
		f.seqs[op.StreamSeq] = true
		f.values[op.Value] = true
		if op.StreamSeq > f.last {
			f.last, f.lastOp = op.StreamSeq, op.Index
		}
	}
	return f
}

func checkFinalRead(ops []history.Op, final *finalRead) []string {
	if final == nil {
		return []string{"no final read, so the stream's contents are unknown"}
	}
	acked := map[uint64]bool{}
	for _, op := range ops {
		if op.F == history.Publish && op.Type == history.OK {
			acked[op.StreamSeq] = true
		}
	}
	var holes, first uint64
	for q := uint64(1); q <= final.last; q++ {
		if final.seqs[q] || acked[q] {
			continue
		}
		if holes == 0 {
			first = q
		}
		holes++
	}
	if holes == 0 {
		return nil
	}
	return []string{fmt.Sprintf("final read has a hole no acked publish explains: %d of %d stream sequences missing, starting at %d", holes, final.last, first)}
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

type progress struct {
	reached uint64 // highest stream_seq delivered before the final read
	lastOp  int    // history index of its latest subscribe or delivery
}

func checkStalls(ops []history.Op, final *finalRead) []Anomaly {
	consumers := map[string]*progress{}
	var order []string
	for _, op := range ops[:final.start] {
		subscribed := op.F == history.Subscribe && op.Type == history.OK
		if !subscribed && op.F != history.Deliver {
			continue
		}
		p := consumers[op.Process]
		if p == nil {
			p = &progress{}
			consumers[op.Process] = p
			order = append(order, op.Process)
		}
		p.lastOp = op.Index
		if op.F == history.Deliver {
			p.reached = max(p.reached, op.StreamSeq)
		}
	}
	var out []Anomaly
	for _, name := range order {
		p := consumers[name]
		if p.reached >= final.last {
			continue
		}
		msg := fmt.Sprintf("had reached stream_seq %d of %d when the final read began", p.reached, final.last)
		if p.reached == 0 {
			msg = fmt.Sprintf("had received nothing when the final read began; the stream ends at stream_seq %d", final.last)
		}
		out = append(out, Anomaly{Kind: Stall, Process: name, Ops: []int{p.lastOp, final.lastOp}, Msg: msg})
	}
	return out
}

func checkLostWrites(ops []history.Op, final *finalRead) []Anomaly {
	var out []Anomaly
	for _, op := range ops {
		if op.F != history.Publish || op.Type != history.OK || final.values[op.Value] {
			continue
		}
		out = append(out, Anomaly{
			Kind:    LostWrite,
			Process: op.Process,
			Ops:     []int{op.Index},
			Msg:     fmt.Sprintf("value %d was acked at stream_seq %d but is missing from the final read", op.Value, op.StreamSeq),
		})
	}
	return out
}

func checkPhantoms(ops []history.Op) []Anomaly {
	invoked := map[uint64]bool{}
	failed := map[uint64]bool{}
	for _, op := range ops {
		if op.F != history.Publish {
			continue
		}
		switch op.Type {
		case history.Invoke:
			invoked[op.Value] = true
		case history.Fail:
			failed[op.Value] = true
		}
	}
	var out []Anomaly
	at := map[uint64]int{} // value -> position of its anomaly in out
	for _, op := range ops {
		if op.F != history.Deliver && op.F != history.Read {
			continue
		}
		if invoked[op.Value] && !failed[op.Value] {
			continue
		}
		if i, ok := at[op.Value]; ok {
			out[i].Ops = append(out[i].Ops, op.Index)
			continue
		}
		msg := fmt.Sprintf("value %d was never published", op.Value)
		if failed[op.Value] {
			msg = fmt.Sprintf("value %d appeared although its publish failed", op.Value)
		}
		at[op.Value] = len(out)
		out = append(out, Anomaly{Kind: Phantom, Process: op.Process, Ops: []int{op.Index}, Msg: msg})
	}
	return out
}
