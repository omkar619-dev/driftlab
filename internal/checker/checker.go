// Package checker judges a parsed history against the guarantees in
// DESIGN.md. It never touches the network: evidence in, verdict out.
package checker

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/omkar619-dev/driftlab/internal/history"
)

// Verdict is a run's overall outcome.
type Verdict string

const (
	Clean   Verdict = "clean"
	Failed  Verdict = "failed"
	Invalid Verdict = "invalid"
)

// Kind names an anomaly. Where Jepsen's Kafka workload has a name for a
// failure, we use that name; the rest are driftlab's own. Jepsen counts a
// repeated delivery as a nonmonotonic poll, but receiving a message twice
// and receiving one out of order are different failures, so
// DuplicateDelivery reports the first on its own.
type Kind string

const (
	PollSkip            Kind = "poll-skip"
	NonmonotonicPoll    Kind = "nonmonotonic-poll"
	DuplicateDelivery   Kind = "duplicate-delivery"
	Stall               Kind = "stall"
	LostWrite           Kind = "lost-write"
	Phantom             Kind = "phantom"
	Duplicate           Kind = "duplicate"
	InconsistentOffsets Kind = "inconsistent-offsets"
	NonmonotonicSend    Kind = "nonmonotonic-send"
	NonmonotonicCopy    Kind = "nonmonotonic-copy"
	BeforeStart         Kind = "before-start"
)

// Anomaly is one broken promise, with the history indexes that prove it.
type Anomaly struct {
	Kind    Kind
	Process string
	Ops     []int
	Msg     string
}

// Recovery is how long one consumer took, after the last fault ended, to
// receive everything up to the last message acked by then, in every
// partition. It is a measurement, and it never affects the verdict
// (DESIGN.md principle 4).
type Recovery struct {
	Process string
	Caught  bool
	After   time.Duration
}

// Lag is the longest one consumer made an acked message wait: the time from
// the publish's ack to the consumer's first delivery of that message. It
// measures what a user of the consumer feels, how stale its view of the
// stream got, wherever the faults fell. A publish that ended as info has no
// ack time, so it doesn't count, and neither does a message the consumer
// never received: the stall and poll-skip anomalies cover those. Like
// Recovery, it is a measurement and never affects the verdict.
type Lag struct {
	Process string
	Value   uint64
	Wait    time.Duration
}

// Result is everything the checker concluded about one history. Reasons
// says why a run is invalid; Unknown counts publishes that ended as info.
type Result struct {
	Verdict    Verdict
	Anomalies  []Anomaly
	Reasons    []string
	Publishes  int
	Unknown    int
	Recoveries []Recovery
	Lags       []Lag
}

// Lines renders r the way the command line prints it: the verdict, then
// why the run is invalid, then each anomaly, then each recovery and lag.
func (r Result) Lines() []string {
	out := []string{fmt.Sprintf("verdict %s, %d of %d publishes unknown", r.Verdict, r.Unknown, r.Publishes)}
	for _, reason := range r.Reasons {
		out = append(out, "invalid: "+reason)
	}
	for _, a := range r.Anomalies {
		out = append(out, fmt.Sprintf("%s %s ops=%v: %s", a.Kind, a.Process, a.Ops, a.Msg))
	}
	for _, rc := range r.Recoveries {
		if rc.Caught {
			out = append(out, fmt.Sprintf("recovery %s: caught up %s after the faults ended", rc.Process, rc.After.Round(time.Millisecond)))
		} else {
			out = append(out, fmt.Sprintf("recovery %s: never caught up after the faults ended", rc.Process))
		}
	}
	for _, l := range r.Lags {
		out = append(out, fmt.Sprintf("lag %s: the longest wait from an ack to its delivery was %s (value %d)", l.Process, l.Wait.Round(time.Millisecond), l.Value))
	}
	return out
}

// Check judges h: invalid if the run can't count as evidence, failed if it
// broke a promise, clean otherwise. The ordered-consumer contract only
// holds for a contiguous stream, so a hole in the final read that no acked
// publish explains makes the run invalid (DESIGN.md principle 5).
func Check(h *history.History) Result {
	n := numbering{kafka: h.Kafka}
	relay := h.Meta.Relay
	final := readFinal(h.Ops)
	r := Result{Anomalies: checkOrdered(h.Ops, n)}
	r.Anomalies = append(r.Anomalies, checkSends(h.Ops, n)...)
	if final != nil {
		r.Anomalies = append(r.Anomalies, checkStalls(h.Ops, final, n)...)
		r.Anomalies = append(r.Anomalies, checkLostWrites(h.Ops, final, n, relay)...)
		// A relay run's acks give positions in the source, and its final read
		// gives positions in the copy, so the two can't be compared. The
		// copy's order is checked instead.
		if relay {
			r.Anomalies = append(r.Anomalies, checkCopies(h.Ops, final, n)...)
		} else {
			r.Anomalies = append(r.Anomalies, checkInconsistentOffsets(h.Ops, final, n)...)
		}
		r.Anomalies = append(r.Anomalies, checkDuplicates(h.Ops, final, n)...)
	}
	r.Anomalies = append(r.Anomalies, checkPhantoms(h.Ops)...)
	r.Anomalies = append(r.Anomalies, checkBeforeStart(h.Ops, h.Meta.StartSeq)...)
	r.Reasons = append(checkFinalRead(h.Ops, final, n, relay), checkFaults(h)...)
	// Both measurements match acks to deliveries by position, so relay runs
	// go without them until they record deliveries that can be matched by
	// value.
	if !relay {
		r.Recoveries = measureRecovery(h.Ops, n)
		r.Lags = measureLag(h.Ops)
	}
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

// numbering is how a history numbers stored messages. A NATS stream counts
// from 1, and every Kafka partition counts from 0.
type numbering struct {
	kafka bool
}

// first is the offset every partition starts at.
func (n numbering) first() uint64 {
	if n.kafka {
		return 0
	}
	return 1
}

// at names a position the way the history writes it.
func (n numbering) at(p history.Pos) string {
	if n.kafka {
		return fmt.Sprintf("partition %d offset %d", p.Partition, p.Offset)
	}
	return fmt.Sprintf("stream_seq %d", p.Offset)
}

type finalRead struct {
	at     map[history.Pos]int // position -> history index of the record there
	values map[uint64]bool     // values it returned
	tails  map[uint32]tail     // partition -> the last record it returned there
	start  int                 // history index of its first record
}

// tail is the last record a final read returned from one partition.
type tail struct {
	offset uint64
	op     int // its history index
}

func readFinal(ops []history.Op) *finalRead {
	var f *finalRead
	for _, op := range ops {
		if op.F != history.Read {
			continue
		}
		if f == nil {
			f = &finalRead{at: map[history.Pos]int{}, values: map[uint64]bool{}, tails: map[uint32]tail{}, start: op.Index}
		}
		p, _ := op.Pos()
		f.at[p] = op.Index
		f.values[op.Value] = true
		if t, ok := f.tails[p.Partition]; !ok || p.Offset > t.offset {
			f.tails[p.Partition] = tail{offset: p.Offset, op: op.Index}
		}
	}
	return f
}

// partitions lists the partitions the final read returned, in order, so
// whatever is reported per partition comes out in the same order every run.
func (f *finalRead) partitions() []uint32 {
	return slices.Sorted(maps.Keys(f.tails))
}

func checkFinalRead(ops []history.Op, final *finalRead, n numbering, relay bool) []string {
	if final == nil {
		return []string{"no final read, so the stream's contents are unknown"}
	}
	// An ack explains a hole at its own position. A relay run's acks give
	// positions in the source, which say nothing about the copy, so none of
	// them explains a hole in it.
	acked := map[history.Pos]bool{}
	for _, op := range ops {
		if op.F == history.Publish && op.Type == history.OK && !relay {
			p, _ := op.Pos()
			acked[p] = true
		}
	}
	var reasons []string
	for _, part := range final.partitions() {
		last := final.tails[part].offset
		var holes, start uint64
		for q := n.first(); q <= last; q++ {
			p := history.Pos{Partition: part, Offset: q}
			if _, ok := final.at[p]; ok || acked[p] {
				continue
			}
			if holes == 0 {
				start = q
			}
			holes++
		}
		if holes == 0 {
			continue
		}
		what := "stream sequences"
		if n.kafka {
			what = fmt.Sprintf("offsets in partition %d", part)
		}
		reasons = append(reasons, fmt.Sprintf("final read has a hole no acked publish explains: %d of %d %s missing, starting at %d",
			holes, last-n.first()+1, what, start))
	}
	return reasons
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
	cursors map[uint32]*cursor  // partition -> its place there
	seen    map[history.Pos]int // position -> history index of its first delivery
}

// cursor is a consumer's place in one partition.
type cursor struct {
	next   uint64 // the offset it should deliver next
	lastOp int    // history index of its latest in-order delivery; -1 before the first
}

type skip struct {
	at      int    // index in the anomaly list
	process string // the consumer that skipped
	part    uint32 // the partition it skipped in
	lo, hi  uint64 // the skipped offsets, inclusive
}

func checkOrdered(ops []history.Op, n numbering) []Anomaly {
	var out []Anomaly
	var skips []skip
	consumers := map[string]*consumer{}
	for _, op := range ops {
		if op.F != history.Deliver {
			continue
		}
		c := consumers[op.Process]
		if c == nil {
			c = &consumer{cursors: map[uint32]*cursor{}, seen: map[history.Pos]int{}}
			consumers[op.Process] = c
		}
		p, _ := op.Pos()
		if earlier, ok := c.seen[p]; ok {
			out = append(out, Anomaly{
				Kind:    DuplicateDelivery,
				Process: op.Process,
				Ops:     []int{earlier, op.Index},
				Msg:     fmt.Sprintf("%s delivered again", n.at(p)),
			})
			continue
		}
		c.seen[p] = op.Index
		cur := c.cursors[p.Partition]
		if cur == nil {
			cur = &cursor{next: n.first(), lastOp: -1}
			c.cursors[p.Partition] = cur
		}
		switch {
		case p.Offset+1 < cur.next:
			out = append(out, Anomaly{
				Kind:    NonmonotonicPoll,
				Process: op.Process,
				Ops:     []int{cur.lastOp, op.Index},
				Msg:     fmt.Sprintf("%s delivered after %d", n.at(p), cur.next-1),
			})
			continue
		case p.Offset > cur.next:
			ev := []int{op.Index}
			if cur.lastOp >= 0 {
				ev = []int{cur.lastOp, op.Index}
			}
			skips = append(skips, skip{at: len(out), process: op.Process, part: p.Partition, lo: cur.next, hi: p.Offset - 1})
			out = append(out, Anomaly{Kind: PollSkip, Process: op.Process, Ops: ev})
		}
		cur.next, cur.lastOp = p.Offset+1, op.Index
	}
	for _, sk := range skips {
		seen := consumers[sk.process].seen
		var late uint64
		for q := sk.lo; q <= sk.hi; q++ {
			if _, ok := seen[history.Pos{Partition: sk.part, Offset: q}]; ok {
				late++
			}
		}
		total := sk.hi - sk.lo + 1
		out[sk.at].Msg = fmt.Sprintf("expected %s, got %d: %d skipped, %d delivered late, %d never delivered",
			n.at(history.Pos{Partition: sk.part, Offset: sk.lo}), sk.hi+1, total, late, total-late)
	}
	return out
}

// send is one acked publish, with when its producer invoked it.
type send struct {
	value  uint64
	invoke int         // history index of its invoke
	ack    int         // history index of its ok
	pos    history.Pos // where its ack says it was stored
}

// checkSends reports a producer whose acks place a send at or before the
// send it made just before, in the same partition. franz-go documents that
// records are produced in order per partition when they succeed, and Jepsen
// calls a break in that order a nonmonotonic send. Sends are taken in the
// order the producer invoked them, which is the order it handed them over,
// not the order their acks came back.
func checkSends(ops []history.Op, n numbering) []Anomaly {
	invoked := map[uint64]int{}  // value -> history index of its invoke
	sends := map[string][]send{} // producer -> its acked sends
	var order []string
	for _, op := range ops {
		if op.F != history.Publish {
			continue
		}
		switch op.Type {
		case history.Invoke:
			invoked[op.Value] = op.Index
		case history.OK:
			p, _ := op.Pos()
			if _, ok := sends[op.Process]; !ok {
				order = append(order, op.Process)
			}
			sends[op.Process] = append(sends[op.Process], send{value: op.Value, invoke: invoked[op.Value], ack: op.Index, pos: p})
		}
	}
	var out []Anomaly
	for _, name := range order {
		ss := sends[name]
		slices.SortFunc(ss, func(a, b send) int { return a.invoke - b.invoke })
		last := map[uint32]send{} // partition -> the send made just before, there
		for _, s := range ss {
			prev, ok := last[s.pos.Partition]
			if ok && s.pos.Offset <= prev.pos.Offset {
				out = append(out, Anomaly{
					Kind:    NonmonotonicSend,
					Process: name,
					Ops:     []int{min(prev.ack, s.ack), max(prev.ack, s.ack)},
					Msg:     fmt.Sprintf("value %d was sent after value %d but acked at %s, not after value %d at %s", s.value, prev.value, n.at(s.pos), prev.value, n.at(prev.pos)),
				})
			}
			last[s.pos.Partition] = s
		}
	}
	return out
}

type progress struct {
	reached map[uint32]uint64 // partition -> highest offset delivered before the final read
	lastOp  int               // history index of its latest subscribe or delivery
}

func checkStalls(ops []history.Op, final *finalRead, n numbering) []Anomaly {
	consumers := map[string]*progress{}
	var order []string
	for _, op := range ops[:final.start] {
		subscribed := op.F == history.Subscribe && op.Type == history.OK
		if !subscribed && op.F != history.Deliver {
			continue
		}
		p := consumers[op.Process]
		if p == nil {
			p = &progress{reached: map[uint32]uint64{}}
			consumers[op.Process] = p
			order = append(order, op.Process)
		}
		p.lastOp = op.Index
		if op.F == history.Deliver {
			pos, _ := op.Pos()
			if got, ok := p.reached[pos.Partition]; !ok || pos.Offset > got {
				p.reached[pos.Partition] = pos.Offset
			}
		}
	}
	var out []Anomaly
	for _, name := range order {
		p := consumers[name]
		for _, part := range final.partitions() {
			end := final.tails[part]
			got, ok := p.reached[part]
			if ok && got >= end.offset {
				continue
			}
			var msg string
			switch {
			case ok:
				msg = fmt.Sprintf("had reached %s of %d when the final read began", n.at(history.Pos{Partition: part, Offset: got}), end.offset)
			case n.kafka:
				msg = fmt.Sprintf("had received nothing from partition %d when the final read began; it ends at offset %d", part, end.offset)
			default:
				msg = fmt.Sprintf("had received nothing when the final read began; the stream ends at stream_seq %d", end.offset)
			}
			out = append(out, Anomaly{Kind: Stall, Process: name, Ops: []int{p.lastOp, end.op}, Msg: msg})
		}
	}
	return out
}

// checkLostWrites reports acked publishes whose values are missing from the
// final read. Jepsen infers lost writes from what consumers read instead, so
// it can't see a message that a consumer received before the log lost it.
// Checking against the final read can. In a relay run the ack is the
// source's and the final read is of the copy, and the message says so.
func checkLostWrites(ops []history.Op, final *finalRead, n numbering, relay bool) []Anomaly {
	var out []Anomaly
	for _, op := range ops {
		if op.F != history.Publish || op.Type != history.OK || final.values[op.Value] {
			continue
		}
		p, _ := op.Pos()
		msg := fmt.Sprintf("value %d was acked at %s but is missing from the final read", op.Value, n.at(p))
		if relay {
			msg = fmt.Sprintf("value %d was acked by the source at %s but is missing from the copy", op.Value, n.at(p))
		}
		out = append(out, Anomaly{
			Kind:    LostWrite,
			Process: op.Process,
			Ops:     []int{op.Index},
			Msg:     msg,
		})
	}
	return out
}

// checkInconsistentOffsets reports acks and deliveries that place a value at
// a position where the final read found a different one. The final read is
// the ground truth, so the claim that disagrees with it is the one reported.
func checkInconsistentOffsets(ops []history.Op, final *finalRead, n numbering) []Anomaly {
	var out []Anomaly
	for _, op := range ops {
		acked := op.F == history.Publish && op.Type == history.OK
		if !acked && op.F != history.Deliver {
			continue
		}
		p, _ := op.Pos()
		i, ok := final.at[p]
		if !ok || ops[i].Value == op.Value {
			continue
		}
		verb := "acked at"
		if !acked {
			verb = "delivered from"
		}
		out = append(out, Anomaly{
			Kind:    InconsistentOffsets,
			Process: op.Process,
			Ops:     []int{op.Index, i},
			Msg:     fmt.Sprintf("value %d was %s %s, but the final read has value %d there", op.Value, verb, n.at(p), ops[i].Value),
		})
	}
	return out
}

// copied is the first copy of a value in a relay run's copy: where the copy
// stores it, and where the source did.
type copied struct {
	value    uint64
	at, from history.Pos
	op       int // history index of the final-read record that found it
}

// checkCopies reports, in a relay run, a value that the copy stores after a
// value the source stored later. The acks say where the source stored each
// value. Walking each partition of the copy from its first offset, the first
// copies of the values from one source partition must sit in the order the
// source stored them. A second copy of a value is left to checkDuplicates,
// and values from different source partitions may mix, because the source
// never ordered them against each other.
func checkCopies(ops []history.Op, final *finalRead, n numbering) []Anomaly {
	source := map[uint64]history.Pos{} // value -> where the source stored it, from its ack
	for _, op := range ops {
		if op.F == history.Publish && op.Type == history.OK {
			p, _ := op.Pos()
			source[op.Value] = p
		}
	}
	var out []Anomaly
	seen := map[uint64]bool{} // values whose first copy has been found
	for _, part := range final.partitions() {
		latest := map[uint32]copied{} // source partition -> its first copy stored latest in the source so far
		for q := n.first(); q <= final.tails[part].offset; q++ {
			at := history.Pos{Partition: part, Offset: q}
			i, ok := final.at[at]
			if !ok {
				continue
			}
			v := ops[i].Value
			from, acked := source[v]
			if !acked || seen[v] {
				continue
			}
			seen[v] = true
			prev, ok := latest[from.Partition]
			if ok && from.Offset < prev.from.Offset {
				out = append(out, Anomaly{
					Kind:    NonmonotonicCopy,
					Process: ops[i].Process,
					Ops:     []int{prev.op, i},
					Msg:     fmt.Sprintf("value %d sits after value %d in the copy (%s, after %s), but before it in the source (%s, before %s)", v, prev.value, n.at(at), n.at(prev.at), n.at(from), n.at(prev.from)),
				})
				continue
			}
			latest[from.Partition] = copied{value: v, at: at, from: from, op: i}
		}
	}
	return out
}

// checkDuplicates reports values that the final read found at more than one
// position. Every value is published once, so a second copy means one write
// was stored twice, which is what an idempotent producer exists to prevent.
// It walks the shelves in order, partition by partition, so each position
// counts once and the report comes out the same every run.
func checkDuplicates(ops []history.Op, final *finalRead, n numbering) []Anomaly {
	where := map[uint64][]history.Pos{} // value -> every position it was found at
	var order []uint64                  // values in the order they were first found
	for _, part := range final.partitions() {
		for q := n.first(); q <= final.tails[part].offset; q++ {
			p := history.Pos{Partition: part, Offset: q}
			i, ok := final.at[p]
			if !ok {
				continue
			}
			v := ops[i].Value
			if len(where[v]) == 0 {
				order = append(order, v)
			}
			where[v] = append(where[v], p)
		}
	}
	var out []Anomaly
	for _, v := range order {
		ps := where[v]
		if len(ps) < 2 {
			continue
		}
		var idxs []int
		var names []string
		for _, p := range ps {
			idxs = append(idxs, final.at[p])
			names = append(names, n.at(p))
		}
		out = append(out, Anomaly{
			Kind:    Duplicate,
			Process: ops[idxs[0]].Process,
			Ops:     idxs,
			Msg:     fmt.Sprintf("value %d is stored at %d positions: %s", v, len(ps), strings.Join(names, ", ")),
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

func measureRecovery(ops []history.Op, n numbering) []Recovery {
	end := -1
	for i, op := range ops {
		if op.Process == history.Nemesis && op.Type == history.OK {
			end = i
		}
	}
	if end < 0 {
		return nil
	}
	// In each partition the target is the highest offset acked before the
	// last fault ended. Offsets have no holes, so a consumer has caught up
	// once it has received every offset from the first one to the target.
	targets := map[uint32]uint64{}
	for _, op := range ops[:end] {
		if op.F != history.Publish || op.Type != history.OK {
			continue
		}
		p, _ := op.Pos()
		if t, ok := targets[p.Partition]; !ok || p.Offset > t {
			targets[p.Partition] = p.Offset
		}
	}
	if len(targets) == 0 {
		return nil
	}
	need := 0
	for _, t := range targets {
		need += int(t - n.first() + 1)
	}

	// For each consumer: its index in out, and which positions up to the
	// targets it has received so far.
	var out []Recovery
	idx := map[string]int{}
	seen := map[string]map[history.Pos]bool{}
	for _, op := range ops {
		if op.F != history.Deliver {
			continue
		}
		i, ok := idx[op.Process]
		if !ok {
			i = len(out)
			idx[op.Process] = i
			seen[op.Process] = map[history.Pos]bool{}
			out = append(out, Recovery{Process: op.Process})
		}
		p, _ := op.Pos()
		t, aimed := targets[p.Partition]
		if out[i].Caught || !aimed || p.Offset > t {
			continue
		}
		seen[op.Process][p] = true
		if len(seen[op.Process]) == need {
			out[i].Caught = true
			out[i].After = max(0, op.Time-ops[end].Time)
		}
	}
	return out
}

// measureLag finds, for each consumer, the acked message that waited longest
// between its ack and the consumer's first delivery of it. Like
// measureRecovery, it only reports on runs where a fault happened.
func measureLag(ops []history.Op) []Lag {
	faulted := slices.ContainsFunc(ops, func(op history.Op) bool {
		return op.Process == history.Nemesis && op.Type == history.OK
	})
	if !faulted {
		return nil
	}
	acked := map[history.Pos]time.Duration{} // position -> when its ack arrived
	for _, op := range ops {
		if op.F == history.Publish && op.Type == history.OK {
			p, _ := op.Pos()
			acked[p] = op.Time
		}
	}

	var out []Lag
	idx := map[string]int{}
	seen := map[string]map[history.Pos]bool{}
	for _, op := range ops {
		if op.F != history.Deliver {
			continue
		}
		p, _ := op.Pos()
		at, ok := acked[p]
		if !ok || seen[op.Process][p] {
			continue
		}
		// A consumer can receive a message just before its ack gets back to
		// the producer, so a negative wait counts as none.
		wait := max(0, op.Time-at)
		i, known := idx[op.Process]
		if !known {
			i = len(out)
			idx[op.Process] = i
			seen[op.Process] = map[history.Pos]bool{}
			out = append(out, Lag{Process: op.Process, Value: op.Value, Wait: wait})
		} else if wait > out[i].Wait {
			out[i].Value, out[i].Wait = op.Value, wait
		}
		seen[op.Process][p] = true
	}
	return out
}

// checkBeforeStart reports deliveries at or below the position the consumer
// asked to start after. A consumer that asked only for new messages, or for a
// start sequence or time, was never promised what came before it: those
// messages are not late, they were never wanted.
//
// Every other check here asks about the sequence that arrived, so a reset that
// forgets the start position and replays from the beginning passes all of them
// - the replay is ordered, gapless and matches the stream. This is the only
// check that asks whether the consumer wanted the messages at all.
func checkBeforeStart(ops []history.Op, startSeq uint64) []Anomaly {
	if startSeq == 0 {
		return nil
	}
	type span struct {
		a         *Anomaly
		low, high uint64
	}
	spans := map[string]*span{}
	var order []string
	for _, op := range ops {
		if op.F != history.Deliver || op.Type != history.OK {
			continue
		}
		if op.StreamSeq == 0 || op.StreamSeq > startSeq {
			continue
		}
		s, ok := spans[op.Process]
		if !ok {
			s = &span{
				a:    &Anomaly{Kind: BeforeStart, Process: op.Process},
				low:  op.StreamSeq,
				high: op.StreamSeq,
			}
			spans[op.Process] = s
			order = append(order, op.Process)
		}
		s.a.Ops = append(s.a.Ops, op.Index)
		if op.StreamSeq < s.low {
			s.low = op.StreamSeq
		}
		if op.StreamSeq > s.high {
			s.high = op.StreamSeq
		}
	}
	out := make([]Anomaly, 0, len(order))
	for _, process := range order {
		s := spans[process]
		if s.low == s.high {
			s.a.Msg = fmt.Sprintf("delivered stream_seq %d after asking to start at stream_seq %d",
				s.low, startSeq+1)
		} else {
			s.a.Msg = fmt.Sprintf("delivered stream_seq %d to %d after asking to start at stream_seq %d",
				s.low, s.high, startSeq+1)
		}
		out = append(out, *s.a)
	}
	return out
}
