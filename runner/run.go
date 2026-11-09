// A run: everything the runner does, from getting the cluster ready to the
// final read, in the order DESIGN.md's Driver split sets out under "A run".
// What a run needs from Kafka is behind the cluster interface, so the tests
// can carry out whole runs against a fake cluster, with a fake driver.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"
)

// plan is one run, as main.go works it out from the flags and the scenario.
type plan struct {
	scenario   string            // the scenario's name, for the meta line
	topic      string            // the run's own topic, a new one for each run
	n          uint64            // how many values to send
	partitions int32             // how many partitions the topic has
	interval   time.Duration     // the pause after each value is taken
	reader     bool              // the driver reads the topic while it is written
	settings   string            // the named set of producer settings
	options    map[string]string // the scenario's changes to single settings
	fault      fault             // the scenario's fault, if it has one
	grace      time.Duration     // how long the reader gets to catch up
	brokers    []string          // where the driver's clients start from
	server     string            // the broker image, for the meta line
	witness    string            // the franz-go the witness reads with
	driftlab   string            // the commit the runner was built from
	driver     []string          // the driver's program, then its arguments
	deadline   time.Duration     // bounds every wait on the driver
}

// fault is a scenario's fault: kind is "cut", "pause" or "kill", or "" for a
// run with none. It starts just before value at is sent, and lasts hold. A
// cut or a freeze hits the first followers of the brokers that hold
// partition 0's other copies, or all of them when followers is 0. A live
// fault starts on a goroutine of its own, while values keep going out; any
// other starts on the loop, which sends nothing until the fault is in place
// (DESIGN.md, Every table fault starts live).
type fault struct {
	kind      string
	followers int
	at        uint64
	hold      time.Duration
	live      bool
}

// activeFault is a fault in place. over closes once it has ended and healed
// what it did, with err set if something went wrong.
type activeFault struct {
	over chan struct{}
	err  error
}

// cluster is what a run needs from Kafka, beyond the driver. kafka.go and
// faults.go will give the real one, which works through franz-go and docker;
// the tests give a fake.
type cluster interface {
	// prepare waits for three live brokers, creates the topic, waits until
	// each of its partitions has a leader, and clears any fault of this kind
	// that an earlier run left behind.
	prepare(ctx context.Context, topic string, partitions int32, kind string) error

	// startFault puts a fault in place and records its lines. It returns once
	// the fault has taken hold. The fault then ends by itself, once its hold
	// is over, and heals what it did.
	startFault(ctx context.Context, rec *recorder, topic string, f fault) (*activeFault, error)

	// ends says where each partition ends: the offset after its last record.
	ends(ctx context.Context, topic string, partitions int32) (map[int32]int64, error)

	// readBack reads every partition back with the witness, from its start
	// to its end, and records each record as a final-read line.
	readBack(ctx context.Context, rec *recorder, topic string, partitions int32) error
}

// run carries out one run, and writes its history to out. The driver's
// stderr, and the runner's own report at the end, go to logs.
//
// It returns an error if the run couldn't be carried out as planned: the
// cluster wasn't ready, the fault failed, the driver broke, or the final
// read did. A driver that broke after its ready leaves a history that says
// how, with no final read, so the checker calls the run invalid. One that
// broke before its ready leaves no history: no run began (DESIGN.md, When a
// driver breaks).
func run(p plan, cl cluster, out, logs io.Writer) error {
	ctx := context.Background()
	rec := newRecorder(out, p.metaLine())

	// 1. The cluster: three live brokers, the run's topic with a leader for
	// each partition, and no fault left over from an earlier run.
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := cl.prepare(pctx, p.topic, p.partitions, p.fault.kind)
	cancel()
	if err != nil {
		return err
	}

	// 2. The driver. Each event it writes is recorded as soon as it is
	// read, on the history's clock, and each delivery also moves the
	// reader's progress on.
	pr := newProgress()
	d, err := startDriver(p.driver, func(e event) {
		rec.got(e)
		pr.saw(e)
	}, logs, rec.clock, p.deadline)
	if err != nil {
		return err
	}
	// ft is the fault, once it has started. trips holds how long each
	// publish took to be taken, for the pipe check, which comes last.
	var ft *activeFault
	var trips []time.Duration
	defer func() { pipeCheck(logs, trips) }()

	// send records a command, sends it, and waits for its answer.
	send := func(c command) error {
		rec.sent(c)
		_, err := d.ask(c)
		return err
	}
	// broke ends a run whose driver broke, and has been stopped: it records
	// how, then waits for the fault to end, so no broker is left faulted.
	// There is no final read.
	broke := func(err error) error {
		rec.broke(err)
		err = fmt.Errorf("the driver broke: %w", err)
		if ft != nil {
			<-ft.over
			return errors.Join(err, ft.err)
		}
		return err
	}

	// The driver makes its producer, and its ready writes the meta line.
	if err := send(newSetup(p.brokers, p.topic, int(p.partitions), p.settings, p.options)); err != nil {
		return broke(err)
	}

	// 3. The reader, if the scenario has one.
	if p.reader {
		if err := send(subscribeCmd); err != nil {
			return broke(err)
		}
	}

	// 4. The values, one publish at a time, value v to partition (v - 1)
	// mod the partition count. After each is taken, the run pauses for the
	// interval, so a client that stops taking values shows as a gap between
	// them (DESIGN.md, Pace). The fault starts just before its value.
	start := func() (*activeFault, error) {
		return cl.startFault(ctx, rec, p.topic, p.fault)
	}
	for v := uint64(1); v <= p.n; v++ {
		if p.fault.kind != "" && v == p.fault.at {
			if p.fault.live {
				ft = startLive(start)
			} else if ft, err = start(); err != nil {
				d.kill()
				return err
			}
		}
		pub := newPublish(v, int32((v-1)%uint64(p.partitions)))
		rec.sent(pub)
		began := time.Now()
		if _, err := d.ask(pub); err != nil {
			return broke(err)
		}
		trips = append(trips, time.Since(began))
		time.Sleep(p.interval)
	}

	// 5. Close the writer, which the driver does once it has reported on
	// every value, and wait for the fault to end. A live fault that failed
	// is reported only now, as in the drivers.
	if err := send(closeWriterCmd); err != nil {
		return broke(err)
	}
	if ft != nil {
		<-ft.over
		if ft.err != nil {
			d.kill()
			return ft.err
		}
	}

	// 6. Give the reader until it reaches the end of the topic, or until
	// the grace runs out, and stop it. Then close the driver's stdin, and
	// wait for it to exit.
	if p.reader {
		waitForReader(ctx, cl, pr, p.topic, p.partitions, p.grace)
		if err := send(stopReaderCmd); err != nil {
			return broke(err)
		}
	}
	if err := d.finish(); err != nil {
		return broke(err)
	}

	// 7. Read the topic back with the witness.
	if err := cl.readBack(ctx, rec, p.topic, p.partitions); err != nil {
		return err
	}
	return rec.Err()
}

// metaLine is the meta line as far as the runner knows it; the driver's
// ready adds the rest. A run whose driver reads says how: "direct", every
// partition from the start, with no group (DESIGN.md, Commands). A run with
// a fault declares it, so if the fault then leaves no evidence, the checker
// calls the run invalid instead of clean.
func (p plan) metaLine() meta {
	m := meta{
		Server:   p.server,
		Scenario: p.scenario,
		Params: params{
			N:          p.n,
			Partitions: int(p.partitions),
			Topic:      p.topic,
			Interval:   p.interval.String(),
			Settings:   p.settings,
			Options:    p.options,
			Witness:    p.witness,
		},
		Driftlab: p.driftlab,
	}
	if p.reader {
		m.API = "direct"
	}
	if p.fault.kind != "" {
		m.Faults = []string{p.fault.kind}
		m.Params.Hold = p.fault.hold.String()
		m.Params.Live = p.fault.live
	}
	return m
}

// startLive starts a fault on a goroutine of its own and returns at once, so
// values keep going out while docker acts on a broker. The fault it returns
// ends when the started one does, and carries any error from starting it,
// which the run reports once the writer is closed.
func startLive(start func() (*activeFault, error)) *activeFault {
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		started, err := start()
		if err != nil {
			ft.err = err
			return
		}
		<-started.over
		ft.err = started.err
	}()
	return ft
}

// progress is how far the driver's reader has got: the highest offset it has
// delivered on each partition. The goroutine that reads the driver's stdout
// moves it on, and the run reads it, so a lock guards it.
type progress struct {
	mu      sync.Mutex
	reached map[int32]int64 // partition -> the highest offset delivered
}

func newProgress() *progress {
	return &progress{reached: map[int32]int64{}}
}

// saw moves the progress on, if the event is a delivery past it.
func (pr *progress) saw(e event) {
	if e.Event != "deliver" {
		return
	}
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if got, ok := pr.reached[e.Partition]; !ok || e.Offset > got {
		pr.reached[e.Partition] = e.Offset
	}
}

// reachedEnds says whether the reader has delivered the last offset of every
// partition, the one before its end. An empty partition has nothing to
// deliver.
func (pr *progress) reachedEnds(ends map[int32]int64) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	for p, end := range ends {
		if got, ok := pr.reached[p]; end > 0 && (!ok || got < end-1) {
			return false
		}
	}
	return true
}

// waitForReader waits until the reader has delivered every partition up to
// where the witness says it ends, and the ends haven't moved when it asks
// again, or until grace runs out. It waits on the log, not on
// acknowledgements: the checker holds the reader to everything the final
// read finds, acknowledged or not. It only decides when the reader is
// stopped; whether the reader got there is the checker's call.
func waitForReader(ctx context.Context, cl cluster, pr *progress, topic string, partitions int32, grace time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	var prev map[int32]int64 // the ends the reader was last found to have reached
	for {
		ends, err := cl.ends(ctx, topic, partitions)
		if err == nil && pr.reachedEnds(ends) {
			if maps.Equal(ends, prev) {
				return
			}
			prev = ends
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// pipeCheck writes, at the end of the runner's stderr, how long a publish
// took to be taken: the median and the slowest. That round trip crosses the
// pipe both ways, and the next value waits for it, so it shows how much the
// split slows the stream (DESIGN.md, The pipe check).
func pipeCheck(logs io.Writer, trips []time.Duration) {
	if len(trips) == 0 {
		return
	}
	slices.Sort(trips)
	median, slowest := trips[len(trips)/2], trips[len(trips)-1]
	fmt.Fprintf(logs, "pipe check: publish to taken took %v at the median and %v at the slowest, over %d values\n",
		median.Round(time.Microsecond), slowest.Round(time.Microsecond), len(trips))
}
