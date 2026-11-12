// Command thin is driftlab's thin sarama driver: sarama's writer and reader,
// and nothing more (DESIGN.md, Driver split). The runner, bin/driftlab-run,
// starts it as a child process and does everything else, from creating the
// topic to the faults and the final read. The runner writes commands on the
// driver's stdin, and the driver answers them on its stdout, one JSON object
// per line. An error that stops the driver goes to stderr, never stdout.
//
//	go build -C drivers/sarama -o ../../bin/sarama-thin ./thin
//	bin/driftlab-run -scenario control -settings idempotent -- bin/sarama-thin
//
// It sits beside drivers/sarama's own command, which still does both jobs,
// until the runner has been calibrated with it (DESIGN.md, Proof). Both are
// in one module, so the same go.mod files pin their sarama.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// protocol is the protocol this driver speaks. setup carries the runner's
// number, and a driver refuses a number it doesn't know (DESIGN.md, What
// stays).
const protocol = 1

func main() {
	d := &driver{
		out:         &events{enc: json.NewEncoder(os.Stdout)},
		client:      client(),
		newProducer: sarama.NewAsyncProducer,
	}
	if err := d.serve(os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, "sarama-thin:", err)
		os.Exit(1)
	}
}

// driver is the driver's state from one command to the next. The runner
// sends a command only once the last one is answered, so serve's goroutine
// is the only one that reads or changes it.
type driver struct {
	out    *events // where events go: stdout, or a buffer in the tests
	client string  // the sarama this binary links, for ready

	// newProducer makes sarama's producer: sarama.NewAsyncProducer, or
	// sarama's mock in the tests.
	newProducer func(brokers []string, cfg *sarama.Config) (sarama.AsyncProducer, error)

	ready bool    // setup is done, and ready written
	w     *writer // sarama's producer, which setup made
}

// command is one of the runner's commands, with the fields any of them can
// carry. Each command fills in its own and leaves the others at zero.
type command struct {
	Cmd        string            `json:"cmd"`
	Protocol   int               `json:"protocol"`
	Brokers    []string          `json:"brokers"`
	Topic      string            `json:"topic"`
	Partitions int32             `json:"partitions"`
	Settings   string            `json:"settings"`
	Options    map[string]string `json:"options"`
	Value      uint64            `json:"value"`
	Partition  int32             `json:"partition"`
}

// event is one line the driver writes, and carries only its own event's
// fields. Partition, Offset and Code are pointers, so omitempty leaves them
// out only when they are absent: partition 0 and offset 0 are real
// positions. Values and polls count from 1, so 0 means absent there.
type event struct {
	Event     string            `json:"event"`
	Driver    string            `json:"driver,omitempty"`
	Client    string            `json:"client,omitempty"`
	Settings  map[string]string `json:"settings,omitempty"`
	Value     uint64            `json:"value,omitempty"`
	Partition *int32            `json:"partition,omitempty"`
	Offset    *int64            `json:"offset,omitempty"`
	Poll      uint64            `json:"poll,omitempty"`
	Process   string            `json:"process,omitempty"`
	Error     string            `json:"error,omitempty"`
	Code      *int              `json:"code,omitempty"`
}

// events writes the driver's events to stdout, one JSON object per line.
// Every line is written under one lock, so lines from two goroutines never
// mix (rule 7), and in one write, straight to stdout, which holds nothing
// back, so each line is flushed as it is written (rule 1).
type events struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// write writes one event. Once a write has failed, every later one fails
// with the same error, which the json.Encoder keeps.
func (o *events) write(e event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.enc.Encode(e)
}

// serve reads the runner's commands, one per line, and carries out each in
// turn. It returns nil once stdin ends, as it does when the runner closes it
// or dies (rule 8). At a line it can't carry out, it returns an error, which
// main writes to stderr before the driver exits with status 1, so the runner
// finds a driver that exited early, and records that it broke.
func (d *driver) serve(in io.Reader) error {
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		var c command
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return fmt.Errorf("reading %q: %w", sc.Bytes(), err)
		}
		if err := d.do(c); err != nil {
			return fmt.Errorf("command %q: %w", c.Cmd, err)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("stdin: %w", err)
	}
	return nil
}

// do carries out one command. setup comes first, and only once (rule 8).
func (d *driver) do(c command) error {
	switch {
	case c.Cmd == "setup" && d.ready:
		return errors.New("came twice")
	case c.Cmd != "setup" && !d.ready:
		return errors.New("came before setup")
	}
	switch c.Cmd {
	case "setup":
		return d.setup(c)
	case "publish":
		return d.w.publish(c.Value, c.Partition)
	case "close-writer":
		return d.w.close()
	default:
		return errors.New("no such command")
	}
}

// setup makes sarama's producer, with the named set of settings and the
// scenario's options, and answers ready: the driver's name, the sarama it
// links, and the value in force of each setting a scenario can change. The
// writer starts reporting outcomes only once ready is written, so ready is
// the first line (rule 8).
func (d *driver) setup(c command) error {
	if c.Protocol != protocol {
		return fmt.Errorf("protocol %d, but this driver speaks %d", c.Protocol, protocol)
	}
	if len(c.Brokers) == 0 || c.Topic == "" || c.Partitions < 1 {
		return errors.New("needs brokers, a topic and at least one partition")
	}
	cfg, err := producerConfig(c.Settings, c.Options)
	if err != nil {
		return err
	}
	p, err := d.newProducer(c.Brokers, cfg)
	if err != nil {
		return fmt.Errorf("starting sarama's producer: %w", err)
	}
	d.w = &writer{p: p, out: d.out, topic: c.Topic, partitions: c.Partitions, done: make(chan struct{})}
	d.ready = true
	if err := d.out.write(event{Event: "ready", Driver: "sarama", Client: d.client, Settings: inForce(cfg)}); err != nil {
		return err
	}
	go d.w.report()
	return nil
}

// producerConfig is sarama's configuration for a named set of settings, with
// a scenario's options applied. Three more settings only serve the driver,
// whatever the name: successes and failures both come back on channels, so
// the outcome of every value is reported, and each value goes to the
// partition the runner gave it.
//
// defaults is sarama as it ships: acknowledgements from the leader alone
// (WaitForLocal), no idempotence, and up to 5 requests in flight per
// connection. sarama promises neither one copy nor, its Net.MaxOpenRequests
// docs say, order with those settings, and Kafka's docs for acks=1 say a
// record is lost if the leader fails after acknowledging it but before a
// follower has copied it.
//
// idempotent turns on sarama's idempotent producer, with the three settings
// sarama requires to go with it: acknowledgements from every in-sync copy,
// one request in flight per broker, and at least one retry, which the default
// of three already gives. Its docs promise that the idempotent producer "will
// ensure that exactly one copy of each message is written"
// (Config.Producer.Idempotent), and the checker holds it to that.
//
// Everything else, the protocol version sarama speaks included, stays at
// sarama's default.
func producerConfig(settings string, options map[string]string) (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	switch settings {
	case "defaults":
	case "idempotent":
		cfg.Producer.Idempotent = true
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Net.MaxOpenRequests = 1
	default:
		return nil, fmt.Errorf("no settings named %q: want defaults or idempotent", settings)
	}
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	if err := applyOptions(cfg, options); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyOptions applies a scenario's options, which are named for what Kafka
// does, not for sarama's fields (DESIGN.md, Settings and options).
// produce_timeout is Producer.Timeout, how long the leader may wait for the
// in-sync copies before it answers REQUEST_TIMED_OUT: a Go duration such as
// 2s, in whole milliseconds, since sarama's docs say it truncates anything
// finer. max_batch_messages is Producer.Flush.MaxMessages, the most messages
// one produce request may carry, at least 1. A name it doesn't know, or a
// value that isn't one, stops the run (rule 6). It takes the names in order,
// so the same options are always refused the same way.
func applyOptions(cfg *sarama.Config, options map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(options)) {
		value := options[name]
		switch name {
		case "produce_timeout":
			t, err := time.ParseDuration(value)
			if err != nil || t <= 0 || t%time.Millisecond != 0 {
				return fmt.Errorf("option produce_timeout=%s: want whole milliseconds above 0, such as 2s", value)
			}
			cfg.Producer.Timeout = t
		case "max_batch_messages":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return fmt.Errorf("option max_batch_messages=%s: want a whole number above 0", value)
			}
			cfg.Producer.Flush.MaxMessages = n
		default:
			return fmt.Errorf("unknown option %q", name)
		}
	}
	return nil
}

// inForce gives, under sarama's own names, the value in force of every
// setting a scenario can change: the three the named set changes, and the
// two the options do, where a Producer.Flush.MaxMessages of 0 means no
// limit. Producer.Retry.Max and Version come too, though no scenario changes
// them: the calibration's timing rests on the retries, and the version
// decides which requests sarama sends.
func inForce(cfg *sarama.Config) map[string]string {
	return map[string]string{
		"Net.MaxOpenRequests":        strconv.Itoa(cfg.Net.MaxOpenRequests),
		"Producer.Flush.MaxMessages": strconv.Itoa(cfg.Producer.Flush.MaxMessages),
		"Producer.Idempotent":        strconv.FormatBool(cfg.Producer.Idempotent),
		"Producer.RequiredAcks":      strconv.Itoa(int(cfg.Producer.RequiredAcks)),
		"Producer.Retry.Max":         strconv.Itoa(cfg.Producer.Retry.Max),
		"Producer.Timeout":           cfg.Producer.Timeout.String(),
		"Version":                    cfg.Version.String(),
	}
}

// client says which sarama this binary links, as Go recorded it at build
// time: the module and its version, such as github.com/IBM/sarama@v1.61.1,
// or the pseudo-version of the commit a -modfile pins. A binary that
// recorded none says unknown.
func client() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/IBM/sarama" {
				return dep.Path + "@" + dep.Version
			}
		}
	}
	return "github.com/IBM/sarama@unknown"
}

// writer is sarama's producer, with what it needs to hand each value over
// and to report what becomes of it.
type writer struct {
	p          sarama.AsyncProducer
	out        *events
	topic      string
	partitions int32
	closed     bool          // close-writer has come
	done       chan struct{} // closed once every value's outcome is reported
}

// publish hands one value to sarama's producer, for the partition the runner
// gave it, and answers taken once sarama has taken it: once the send on the
// producer's Input channel has gone through. The value goes as decimal text
// with no key (rule 4), and rides along in the message's Metadata, which
// sarama hands back with the value's outcome.
func (w *writer) publish(v uint64, partition int32) error {
	switch {
	case w.closed:
		return errors.New("came after close-writer")
	case v == 0:
		return errors.New("values start at 1")
	case partition < 0 || partition >= w.partitions:
		return fmt.Errorf("partition %d, but the topic's partitions run from 0 to %d", partition, w.partitions-1)
	}
	w.p.Input() <- &sarama.ProducerMessage{
		Topic:     w.topic,
		Partition: partition,
		Value:     sarama.StringEncoder(strconv.FormatUint(v, 10)),
		Metadata:  v,
	}
	return w.out.write(event{Event: "taken", Value: v})
}

// report reports what becomes of each value, as sarama hands the outcomes
// back, until sarama has closed both of its channels, which it does once
// close-writer has closed the producer and the last outcome is out. A
// success is an ok, with the partition and offset the broker gave. A
// failure is an info, never a fail (rule 5): sarama's ProducerError says
// only that the producer failed to deliver the message, which doesn't
// promise that the broker never stored it, and a write that timed out may
// still land. Its code is Kafka's error number, when sarama's error is one
// of Kafka's. A failure that carries no value of the run's is an error
// event from the producer, about no one value.
//
// A failed write isn't returned from here. The events' encoder keeps it,
// though, so the answer to close-writer, which comes after the last report,
// fails with it.
func (w *writer) report() {
	defer close(w.done)
	successes, failures := w.p.Successes(), w.p.Errors()
	for successes != nil || failures != nil {
		select {
		case m, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			v, _ := m.Metadata.(uint64)
			p, o := m.Partition, m.Offset
			w.out.write(event{Event: "ok", Value: v, Partition: &p, Offset: &o})
		case e, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			var v uint64
			if e.Msg != nil {
				v, _ = e.Msg.Metadata.(uint64)
			}
			if v == 0 {
				w.out.write(event{Event: "error", Process: "producer", Error: fmt.Sprint(e.Err)})
				continue
			}
			w.out.write(event{Event: "info", Value: v, Error: fmt.Sprint(e.Err), Code: kafkaCode(e.Err)})
		}
	}
}

// close closes sarama's producer, which first sends whatever it still holds,
// and answers writer-closed once every value's outcome has been reported:
// sarama closes its two channels only after the last outcome, and report
// returns only after both have closed.
func (w *writer) close() error {
	if w.closed {
		return errors.New("came twice")
	}
	w.closed = true
	w.p.AsyncClose()
	<-w.done
	return w.out.write(event{Event: "writer-closed"})
}

// kafkaCode is Kafka's error number for err, such as 7 for
// REQUEST_TIMED_OUT, when err is one of the errors a broker returns, and nil
// when it isn't, as for a refused connection. The runner takes the
// calibration's timeout evidence from code 7 (DESIGN.md, Proof).
func kafkaCode(err error) *int {
	var ke sarama.KError
	if !errors.As(err, &ke) {
		return nil
	}
	code := int(ke)
	return &code
}
