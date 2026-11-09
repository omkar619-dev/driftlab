// The history: the runner writes it, a meta line and then one operation per
// line, in the format DESIGN.md's History format sets, from what it sends a
// driver and what the driver reports (DESIGN.md, Driver split, Mapping to
// the history).

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// meta is the history's first line: what produced the run. Driver and Client
// come from the driver's ready, the rest from the runner.
type meta struct {
	Type     string   `json:"type"`
	Driver   string   `json:"driver"`
	Client   string   `json:"client"`
	API      string   `json:"api,omitempty"`
	Server   string   `json:"server"`
	Scenario string   `json:"scenario"`
	Faults   []string `json:"faults,omitempty"`
	Params   params   `json:"params"`
	Driftlab string   `json:"driftlab"`
}

// params describes how the run was set up. Settings names the set of producer
// settings the runner asked for, and SettingsInForce gives the value of each
// setting a scenario can change, as the driver's ready reports it. Options
// are the scenario's own changes to those settings. Hold, how long the fault
// lasts, and Live, that it starts while values are going out, are set only
// in a run with a fault. Witness is the franz-go the runner reads the topic
// back with.
type params struct {
	N               uint64            `json:"n"`
	Partitions      int               `json:"partitions"`
	Topic           string            `json:"topic"`
	Interval        string            `json:"interval"`
	Settings        string            `json:"settings"`
	SettingsInForce map[string]string `json:"settings_in_force"`
	Options         map[string]string `json:"options,omitempty"`
	Hold            string            `json:"hold,omitempty"`
	Live            bool              `json:"live,omitempty"`
	Witness         string            `json:"witness"`
}

// op is one operation: one line after the meta line. Partition, Offset and
// Code are pointers, so omitempty leaves them out only when they are absent:
// partition 0 and offset 0 are real positions, and they get written. Value
// and Poll count from 1, so 0 means absent there.
type op struct {
	Index     int    `json:"index"`
	TimeNS    int64  `json:"time_ns"`
	Process   string `json:"process"`
	Type      string `json:"type"`
	F         string `json:"f"`
	Value     uint64 `json:"value,omitempty"`
	Partition *int32 `json:"partition,omitempty"`
	Offset    *int64 `json:"offset,omitempty"`
	Poll      uint64 `json:"poll,omitempty"`
	Error     string `json:"error,omitempty"`
	Code      *int   `json:"code,omitempty"`
	Detail    any    `json:"detail,omitempty"`
}

// recorder writes the history. It numbers and times each line as it writes
// it, under one lock, so the numbers and the times both follow the order of
// the lines. Its methods can be called from any goroutine: the one reading
// the driver's stdout, the run's own, or a fault's.
type recorder struct {
	// clock is the history's one clock: the time since the recorder was
	// made. The driver's log lines carry it too, as startDriver's clock.
	clock func() time.Duration

	mu        sync.Mutex // guards the fields below
	enc       *json.Encoder
	meta      meta
	wroteMeta bool
	next      int   // the next operation's index
	err       error // the first error, after which nothing more is written
}

// newRecorder makes a recorder that writes to w, and starts its clock. m is
// the meta line as far as the runner knows it; the driver's ready adds the
// rest.
func newRecorder(w io.Writer, m meta) *recorder {
	start := time.Now()
	return &recorder{
		clock: func() time.Duration { return time.Since(start) },
		enc:   json.NewEncoder(w),
		meta:  m,
	}
}

// sent records a command the runner is about to send: a publish's invoke, or
// the consumer's subscribe invoke. The other commands record nothing. The
// run calls it before it writes the command, so the invoke comes first even
// when the driver reports on a value before its send call has returned.
func (r *recorder) sent(c command) {
	switch c := c.(type) {
	case publishCmd:
		r.op(op{Process: "producer", Type: "invoke", F: "publish", Value: c.Value})
	case bareCmd:
		if c.Cmd == "subscribe" {
			r.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
		}
	}
}

// got records an event the driver reported. It is the driver's onEvent, so
// it runs on the goroutine that reads the driver's stdout, as each event
// arrives. taken, writer-closed and reader-stopped record nothing: they only
// answer commands, and sent has already recorded the commands that count.
func (r *recorder) got(e event) {
	switch e.Event {
	case "ready":
		r.writeMeta(e)
	case "subscribed":
		r.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	case "ok":
		p, o := e.Partition, e.Offset
		r.op(op{Process: "producer", Type: "ok", F: "publish", Value: e.Value, Partition: &p, Offset: &o})
	case "info", "fail":
		r.op(op{Process: "producer", Type: e.Event, F: "publish", Value: e.Value, Error: e.Error, Code: e.Code})
	case "deliver":
		p, o := e.Partition, e.Offset
		r.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: e.Value, Partition: &p, Offset: &o, Poll: e.Poll})
	case "error":
		r.op(op{Process: e.Process, Type: "info", F: "client-error", Error: e.Error})
	}
}

// writeMeta writes the meta line, the history's first: what the runner
// knows, plus what ready says, the driver's name, the version of the client
// it loaded, and the settings in force.
func (r *recorder) writeMeta(e event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.meta.Type = "meta"
	r.meta.Driver = e.Driver
	r.meta.Client = e.Client
	r.meta.Params.SettingsInForce = e.Settings
	r.err = r.enc.Encode(r.meta)
	r.wroteMeta = true
}

// broke records how the driver broke, as an info from a driver process
// (DESIGN.md, When a driver breaks). A driver that broke before its ready
// leaves no history at all: with no meta line yet, no run began.
func (r *recorder) broke(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wroteMeta {
		r.write(op{Process: "driver", Type: "info", F: "broke", Error: err.Error()})
	}
}

// op records one operation: one the driver reported, or one of the runner's
// own, such as a fault's or the final read's.
func (r *recorder) op(o op) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.write(o)
}

// write numbers and times an operation, and writes it. The caller holds
// r.mu. An operation before the meta line is a bug in the runner: it isn't
// written, and Err reports it.
func (r *recorder) write(o op) {
	if r.err != nil {
		return
	}
	if !r.wroteMeta {
		r.err = fmt.Errorf("%s %s from %s came before the meta line", o.Type, o.F, o.Process)
		return
	}
	o.Index = r.next
	o.TimeNS = r.clock().Nanoseconds()
	r.next++
	r.err = r.enc.Encode(o)
}

// Err returns the first error: a write that failed, or an operation that
// came before the meta line. Once there is one, nothing more is written.
func (r *recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
