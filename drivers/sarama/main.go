// Command saramadriver runs one driftlab scenario against Kafka through
// sarama and writes its history to stdout, in the format DESIGN.md
// specifies.
//
// sarama does the writing and, in control, the reading while the topic is
// written. Everything else goes through franz-go v1.20.7, pinned in go.mod:
// creating the topic, finding where each partition ends, and the final read,
// so the witness is never the thing being tested. franz-go's admin requests
// go through kmsg, as in drivers/franzgo.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replicas is how many copies of each partition the cluster keeps: one on
// each of the three brokers in deploy/kafka.yaml.
const replicas = 3

// scenario describes one experiment. With consumer set, a solo helper reads
// the topic through sarama while it is being written.
type scenario struct {
	consumer bool
}

var scenarios = map[string]scenario{
	"publish-only": {},
	"control":      {consumer: true},
}

type config struct {
	brokers    string
	server     string
	scenario   string
	n          uint64
	partitions int
	interval   time.Duration
	grace      time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only or control")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic")
	flag.DurationVar(&c.interval, "interval", 10*time.Millisecond, "the pause after handing each value to the producer")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "saramadriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	if c.n < 1 {
		return errors.New("-n must be at least 1")
	}
	if c.partitions < 1 {
		return errors.New("-partitions must be at least 1")
	}
	partitions := int32(c.partitions)
	rec := newRecorder(out)
	ctx := context.Background()

	// The witness's client, franz-go and never sarama. It creates the topic,
	// waits for its leaders, finds where each partition ends and does the
	// final read.
	seeds := strings.Split(c.brokers, ",")
	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer adm.Close()

	topic := fmt.Sprintf("driftlab-sarama-%s-%d", c.scenario, time.Now().UnixMilli())
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := createTopic(setup, adm, topic, partitions); err != nil {
		return fmt.Errorf("%w (is the cluster up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	if err := waitForLeaders(setup, adm, topic, partitions); err != nil {
		return err
	}

	cfg := producerConfig()
	rec.writeMeta(newMeta(c, sc, topic, cfg))
	var h *helper
	if sc.consumer {
		if h, err = startHelper(seeds, rec, topic, partitions); err != nil {
			return err
		}
	}
	w, err := startWriter(seeds, cfg, rec, topic, partitions)
	if err != nil {
		if h != nil {
			h.stop()
		}
		return err
	}
	for v := uint64(1); v <= c.n; v++ {
		w.publish(v)
		time.Sleep(c.interval)
	}
	last := w.close()
	if h != nil {
		h.waitFor(last, c.grace)
		h.stop()
	}
	return finalRead(ctx, adm, seeds, rec, topic, partitions)
}

// producerConfig is sarama's default configuration with its idempotent
// producer turned on, which the defaults leave off. sarama requires three
// settings to go with it, and the run makes exactly those changes: acks from
// every in-sync replica, one request in flight per broker, and at least one
// retry, which the default of three already gives. Its docs promise that the
// idempotent producer "will ensure that exactly one copy of each message is
// written" (Config.Producer.Idempotent), and that is what the checker holds
// it to. Two more settings only serve the driver: successes come back on a
// channel, so every ack is recorded with its position, and each message says
// which partition it goes to. Everything else, the protocol version sarama
// speaks included, stays at sarama's default.
func producerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	return cfg
}

// writer publishes the run's values through sarama's AsyncProducer. A send
// doesn't wait for its ack, so several values can be in flight at once, as
// they are for most sarama users. Value v goes to partition (v-1) mod
// partitions, so a history can be checked by hand, and carries v in its
// Metadata, so its outcome can be matched back to it.
type writer struct {
	p          sarama.AsyncProducer
	rec        *recorder
	topic      string
	partitions int32
	done       chan struct{} // closed once every outcome has been recorded

	mu   sync.Mutex
	last map[int32]int64 // partition -> the highest offset acked so far
}

// startWriter starts the producer and the goroutine that records what
// becomes of each value.
func startWriter(seeds []string, cfg *sarama.Config, rec *recorder, topic string, partitions int32) (*writer, error) {
	p, err := sarama.NewAsyncProducer(seeds, cfg)
	if err != nil {
		return nil, fmt.Errorf("starting sarama's producer: %w", err)
	}
	w := &writer{p: p, rec: rec, topic: topic, partitions: partitions, done: make(chan struct{}), last: map[int32]int64{}}
	go w.results()
	return w, nil
}

// publish records one publish and hands the value to the producer.
func (w *writer) publish(v uint64) {
	w.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	w.p.Input() <- &sarama.ProducerMessage{
		Topic:     w.topic,
		Partition: int32((v - 1) % uint64(w.partitions)),
		Value:     sarama.StringEncoder(strconv.FormatUint(v, 10)),
		Metadata:  v,
	}
}

// results records each value's outcome as sarama reports it, until it has
// closed both channels. A success is an ok, with the partition and offset the
// broker gave. An error is an info, not a fail: sarama's ProducerError says
// the producer failed to deliver the message, which doesn't promise the
// broker never stored it, and a write that timed out may still land.
func (w *writer) results() {
	defer close(w.done)
	successes, errs := w.p.Successes(), w.p.Errors()
	for successes != nil || errs != nil {
		select {
		case m, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			v, _ := m.Metadata.(uint64)
			p, o := m.Partition, m.Offset
			w.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &p, Offset: &o})
			w.mu.Lock()
			if got, ok := w.last[p]; !ok || o > got {
				w.last[p] = o
			}
			w.mu.Unlock()
		case e, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			var v uint64
			if e.Msg != nil {
				v, _ = e.Msg.Metadata.(uint64)
			}
			if v == 0 {
				// Not one of the run's values, so no publish's outcome. Not
				// e.Error(), which reads e.Msg.Topic.
				w.rec.op(op{Process: "producer", Type: "info", F: "client-error", Error: fmt.Sprint(e.Err)})
				continue
			}
			w.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: e.Err.Error()})
		}
	}
}

// close shuts the producer down, which flushes whatever it still holds, and
// waits until every value's outcome has been recorded. It returns the highest
// offset acked in each partition.
func (w *writer) close() map[int32]int64 {
	w.p.AsyncClose()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}

// helper is the solo consumer: sarama's PartitionConsumer on every
// partition, reading from the oldest offset, with no group and no committed
// offsets. sarama hands messages over one at a time, so its deliver lines
// carry no poll number.
type helper struct {
	cons sarama.Consumer
	pcs  []sarama.PartitionConsumer
	wg   sync.WaitGroup

	mu      sync.Mutex
	reached map[int32]int64 // partition -> the highest offset delivered
}

// startHelper starts the helper. It records subscribe around starting every
// partition consumer, because each one starts fetching as soon as it exists.
// Errors come back on a channel, so they can be recorded instead of only
// logged.
func startHelper(seeds []string, rec *recorder, topic string, partitions int32) (*helper, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	cons, err := sarama.NewConsumer(seeds, cfg)
	if err != nil {
		return nil, fmt.Errorf("starting sarama's consumer: %w", err)
	}
	h := &helper{cons: cons, reached: map[int32]int64{}}
	for p := int32(0); p < partitions; p++ {
		pc, err := cons.ConsumePartition(topic, p, sarama.OffsetOldest)
		if err != nil {
			h.stop()
			return nil, fmt.Errorf("consuming partition %d: %w", p, err)
		}
		h.pcs = append(h.pcs, pc)
		h.wg.Add(2)
		go h.deliver(rec, pc)
		go h.clientErrors(rec, pc)
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	return h, nil
}

// deliver records every message one partition consumer hands over, until it
// is closed.
func (h *helper) deliver(rec *recorder, pc sarama.PartitionConsumer) {
	defer h.wg.Done()
	for m := range pc.Messages() {
		p, o := m.Partition, m.Offset
		v, err := strconv.ParseUint(string(m.Value), 10, 64)
		if err != nil {
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: fmt.Sprintf("partition %d offset %d holds %q, not a driftlab value", p, o, m.Value)})
			continue
		}
		rec.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: v, Partition: &p, Offset: &o})
		h.mu.Lock()
		if got, ok := h.reached[p]; !ok || o > got {
			h.reached[p] = o
		}
		h.mu.Unlock()
	}
}

// clientErrors records every error one partition consumer reports, until it
// is closed.
func (h *helper) clientErrors(rec *recorder, pc sarama.PartitionConsumer) {
	defer h.wg.Done()
	for e := range pc.Errors() {
		rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: e.Error()})
	}
}

// waitFor waits until the helper has delivered, in every partition, the last
// offset the producer saw acked, or until grace runs out. It only decides when
// the final read starts; whether the helper got there is the checker's call.
func (h *helper) waitFor(last map[int32]int64, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for !h.reachedAll(last) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *helper) reachedAll(last map[int32]int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for p, o := range last {
		if got, ok := h.reached[p]; !ok || got < o {
			return false
		}
	}
	return true
}

// stop closes every partition consumer, waits until their channels are
// drained, so every delivery it records comes before the final read, and
// closes the consumer.
func (h *helper) stop() {
	for _, pc := range h.pcs {
		pc.AsyncClose()
	}
	h.wg.Wait()
	h.cons.Close()
}

// createTopic creates the run's topic, with a copy of every partition on
// every broker. Each run writes to a topic of its own, so it starts empty,
// and its evidence stays in the cluster for a second reader.
func createTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic = topic
	t.NumPartitions = partitions
	t.ReplicationFactor = replicas
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("creating topic %s: %w", topic, err)
	}
	if len(resp.Topics) != 1 {
		return fmt.Errorf("creating topic %s: got %d topics back", topic, len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil {
		return fmt.Errorf("creating topic %s: %w", topic, err)
	}
	return nil
}

// waitForLeaders waits until every partition of the new topic has a leader.
// Creating a topic returns once the controller has recorded it, which can be
// a moment before every broker knows about it.
func waitForLeaders(ctx context.Context, cl *kgo.Client, topic string, partitions int32) error {
	for {
		req := kmsg.NewPtrMetadataRequest()
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, t)
		resp, err := req.RequestWith(ctx, cl)
		if err == nil && allLed(resp, partitions) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topic %s: its partitions never all got a leader: %w", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// allLed says whether a metadata response shows the topic with every
// partition led by a broker.
func allLed(resp *kmsg.MetadataResponse, partitions int32) bool {
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 || len(resp.Topics[0].Partitions) != int(partitions) {
		return false
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 || p.Leader < 0 {
			return false
		}
	}
	return true
}

// finalRead reads every partition back, from its first offset to its end,
// with a fresh franz-go client of its own: no group and no committed offsets
// (DESIGN.md, v1 design). It first asks where each partition ends, so it
// knows when it has read everything.
func finalRead(ctx context.Context, cl *kgo.Client, seeds []string, rec *recorder, topic string, partitions int32) error {
	ends, err := endOffsets(ctx, cl, topic, partitions)
	if err != nil {
		return err
	}
	start := map[int32]kgo.Offset{}
	left := map[int32]bool{} // partitions not yet read to their end
	for p, end := range ends {
		if end > 0 {
			start[p] = kgo.NewOffset().AtStart()
			left[p] = true
		}
	}
	if len(left) == 0 {
		return rec.Err() // nothing was stored, so there is nothing to read
	}
	rd, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: start}))
	if err != nil {
		return err
	}
	defer rd.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for len(left) > 0 {
		fs := rd.PollFetches(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("final read: %d partitions still unread after 30s", len(left))
		}
		if errs := fs.Errors(); len(errs) > 0 {
			return fmt.Errorf("final read: partition %d: %w", errs[0].Partition, errs[0].Err)
		}
		for it := fs.RecordIter(); !it.Done(); {
			r := it.Next()
			p, o := r.Partition, r.Offset
			if o >= ends[p] {
				continue // written after the end was taken
			}
			v, err := strconv.ParseUint(string(r.Value), 10, 64)
			if err != nil {
				return fmt.Errorf("partition %d offset %d holds %q, not a driftlab value", p, o, r.Value)
			}
			rec.op(op{Process: "final-read", Type: "ok", F: "read", Value: v, Partition: &p, Offset: &o})
			if o == ends[p]-1 {
				delete(left, p)
			}
		}
	}
	return rec.Err()
}

// endOffsets asks where each partition ends: the offset after the last
// record a consumer can read.
func endOffsets(ctx context.Context, cl *kgo.Client, topic string, partitions int32) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for p := int32(0); p < partitions; p++ {
		tp := kmsg.NewListOffsetsRequestTopicPartition()
		tp.Partition = p
		tp.Timestamp = -1 // -1 asks for the latest offset
		t.Partitions = append(t.Partitions, tp)
	}
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("finding where partitions end: %w", err)
	}
	ends := map[int32]int64{}
	for _, rt := range resp.Topics {
		for _, rp := range rt.Partitions {
			if err := kerr.ErrorForCode(rp.ErrorCode); err != nil {
				return nil, fmt.Errorf("finding where partition %d ends: %w", rp.Partition, err)
			}
			ends[rp.Partition] = rp.Offset
		}
	}
	if len(ends) != int(partitions) {
		return nil, fmt.Errorf("found where %d partitions end, want %d", len(ends), partitions)
	}
	return ends, nil
}

// op is one line of the history. Partition and Offset are pointers, so
// omitempty leaves them out only when they are absent: partition 0 and
// offset 0 are real positions, and they get written.
type op struct {
	Index     int    `json:"index"`
	TimeNS    int64  `json:"time_ns"`
	Process   string `json:"process"`
	Type      string `json:"type"`
	F         string `json:"f"`
	Value     uint64 `json:"value,omitempty"`
	Partition *int32 `json:"partition,omitempty"`
	Offset    *int64 `json:"offset,omitempty"`
	Error     string `json:"error,omitempty"`
	Detail    any    `json:"detail,omitempty"`
}

type metaLine struct {
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

type params struct {
	N          uint64 `json:"n"`
	Partitions int    `json:"partitions"`
	Topic      string `json:"topic"`
	Interval   string `json:"interval"`
	Protocol   string `json:"protocol"`
	Witness    string `json:"witness"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards, the Kafka protocol version sarama was told
// to speak, and the franz-go the witness used. A run with a helper also says
// how it consumes: "direct" means partitions assigned by the driver, with no
// group.
func newMeta(c config, sc scenario, topic string, cfg *sarama.Config) metaLine {
	client, witness, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "sarama",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params: params{
			N:          c.n,
			Partitions: c.partitions,
			Topic:      topic,
			Interval:   c.interval.String(),
			Protocol:   cfg.Version.String(),
			Witness:    witness,
		},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = "direct"
	}
	return m
}

// recorder writes the history. Its mutex guards every other field, so the
// producer's and the consumer's goroutines can record too.
type recorder struct {
	mu    sync.Mutex
	enc   *json.Encoder
	start time.Time
	next  int
	err   error
}

func newRecorder(w io.Writer) *recorder {
	return &recorder{enc: json.NewEncoder(w), start: time.Now()}
}

func (r *recorder) writeMeta(m metaLine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = r.enc.Encode(m)
}

// op writes one operation. After the first write error it writes nothing
// more; finalRead returns that error at the end of the run.
func (r *recorder) op(o op) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	o.Index = r.next
	o.TimeNS = time.Since(r.start).Nanoseconds()
	r.next++
	r.err = r.enc.Encode(o)
}

func (r *recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// provenance returns the sarama version linked into this binary, the
// franz-go version its witness uses, and the driftlab commit it was built
// from, so every history says what produced it.
func provenance() (client, witness, rev string) {
	client, witness, rev = "unknown", "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return client, witness, rev
	}
	for _, dep := range info.Deps {
		switch dep.Path {
		case "github.com/IBM/sarama":
			client = dep.Path + "@" + dep.Version
		case "github.com/twmb/franz-go":
			witness = dep.Path + "@" + dep.Version
		}
	}
	dirty := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if dirty {
		rev += "-dirty"
	}
	return client, witness, rev
}
