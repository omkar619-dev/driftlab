// Command confluentdriver runs one driftlab scenario against Kafka through
// confluent-kafka-go, Confluent's Go client, and writes its history to
// stdout, in the format DESIGN.md specifies. confluent-kafka-go wraps
// librdkafka, Kafka's client library in C, and links in a copy built for the
// platform, so the driver is built with cgo and needs a C compiler.
//
// confluent-kafka-go does the writing and, in the scenarios with a helper,
// the reading while the topic is written. Everything else goes through
// franz-go v1.20.7, pinned in go.mod: creating the topic, finding where each
// partition ends, and the final read, so the witness is never the thing
// being tested. franz-go's admin requests go through kmsg, as in
// drivers/franzgo.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replicas is how many copies of each partition the cluster keeps: one on
// each of the three brokers in deploy/kafka.yaml.
const replicas = 3

// scenario describes one experiment. With consumer set, a solo helper reads
// the topic through confluent-kafka-go while it is being written. A non-zero
// partitions or n overrides the -partitions or -n flag, and a non-zero
// interval replaces the pause after each value, 10 milliseconds otherwise.
type scenario struct {
	consumer   bool
	partitions int
	n          uint64
	interval   time.Duration
}

// publish-only writes the topic and reads it back with the witness alone;
// control adds the helper.
var scenarios = map[string]scenario{
	"publish-only": {},
	"control":      {consumer: true},
}

type config struct {
	brokers    string
	server     string
	scenario   string
	settings   string
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
	flag.StringVar(&c.settings, "settings", "idempotent", "the producer's settings: idempotent, librdkafka's idempotent producer (the driver's default); defaults, librdkafka's own, which wait for every in-sync copy but aren't idempotent; or acks-one, the leader's acknowledgement alone and no idempotence, for the settings check")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish, unless the scenario sets it")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	flag.DurationVar(&c.interval, "interval", 0, "the pause after handing each value to the producer; 0 keeps the scenario's own, or 10ms")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "confluentdriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	cfg, err := producerConfig(c.settings, c.brokers)
	if err != nil {
		return err
	}
	if sc.partitions == 0 {
		sc.partitions = c.partitions
	}
	if sc.n == 0 {
		sc.n = c.n
	}
	if c.interval == 0 {
		c.interval = sc.interval
	}
	if c.interval == 0 {
		c.interval = 10 * time.Millisecond
	}
	if sc.n < 1 {
		return errors.New("-n must be at least 1")
	}
	if sc.partitions < 1 {
		return errors.New("-partitions must be at least 1")
	}
	partitions := int32(sc.partitions)
	rec := newRecorder(out)
	ctx := context.Background()

	// The witness's client, franz-go and never confluent-kafka-go. It
	// creates the topic, waits for its leaders, finds where each partition
	// ends, and does the final read.
	seeds := strings.Split(c.brokers, ",")
	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer adm.Close()

	topic := fmt.Sprintf("driftlab-confluent-%s-%d", c.scenario, time.Now().UnixMilli())
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := waitForBrokers(setup, adm, replicas); err != nil {
		return err
	}
	if err := createTopic(setup, adm, topic, partitions); err != nil {
		return fmt.Errorf("%w (is the cluster up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	if err := waitForLeaders(setup, adm, topic, partitions); err != nil {
		return err
	}

	rec.writeMeta(newMeta(c, sc, topic))
	var h *helper
	if sc.consumer {
		if h, err = startHelper(c.brokers, rec, topic, partitions); err != nil {
			return err
		}
	}
	w, err := startWriter(cfg, rec)
	if err != nil {
		if h != nil {
			h.stop()
		}
		return err
	}
	for v := uint64(1); v <= sc.n; v++ {
		w.publish(&topic, partitions, v)
		time.Sleep(c.interval)
	}
	w.close()
	if h != nil {
		h.waitForEnd(ctx, adm, topic, partitions, c.grace)
		h.stop()
	}
	return finalRead(ctx, adm, seeds, rec, topic, partitions)
}

// producerConfig is librdkafka's configuration for -settings. Each one names
// the brokers; confluent-kafka-go sends a delivery report for every message
// by default. idempotent turns on librdkafka's idempotent producer, whose
// docs promise that "the producer will ensure that messages are successfully
// produced exactly once and in the original produce order"; on its own it
// asks for acknowledgements from every in-sync copy and keeps at most 5
// requests in flight per broker. defaults changes nothing: librdkafka already
// waits for every in-sync copy, but without idempotence it retries a message
// for up to 5 minutes, with up to a million requests in flight, and its docs
// say retrying may then reorder messages. acks-one asks for the leader's
// acknowledgement alone, also without idempotence, which Kafka documents can
// lose a write the leader acknowledged if it dies before a follower copies
// it; the settings check holds it to that.
func producerConfig(settings, brokers string) (*kafka.ConfigMap, error) {
	cfg := kafka.ConfigMap{"bootstrap.servers": brokers}
	switch settings {
	case "idempotent":
		cfg["enable.idempotence"] = true
	case "defaults":
	case "acks-one":
		cfg["acks"] = 1
	default:
		return nil, fmt.Errorf("-settings %q: want idempotent, defaults or acks-one", settings)
	}
	return &cfg, nil
}

// writer publishes the run's values through a confluent-kafka-go Producer.
// Produce only queues a message: librdkafka batches and sends it in the
// background, and the message's delivery report, with its position or its
// error, comes back on the producer's Events channel, where reports records
// it.
type writer struct {
	p    *kafka.Producer
	rec  *recorder
	done chan struct{} // closed once reports has recorded the last report
}

// startWriter makes the producer and starts reports.
func startWriter(cfg *kafka.ConfigMap, rec *recorder) (*writer, error) {
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	wr := &writer{p: p, rec: rec, done: make(chan struct{})}
	go wr.reports()
	return wr, nil
}

// publish records one publish and queues the value for partition (v-1) mod
// the number of partitions, so a history can be checked by hand. The message
// carries its value in Opaque, which comes back untouched in its delivery
// report. Produce returns an error only when it can't queue the message, and
// the value is then recorded as an info at once.
func (wr *writer) publish(topic *string, partitions int32, v uint64) {
	wr.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	m := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: int32((v - 1) % uint64(partitions))},
		Value:          []byte(strconv.FormatUint(v, 10)),
		Opaque:         v,
	}
	if err := wr.p.Produce(m, nil); err != nil {
		wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
	}
}

// reports records every delivery report until Close closes the Events
// channel. A report with an error is an info, not a fail: librdkafka gives up
// on a message after retrying it, and an attempt that timed out may still
// have been stored. Otherwise it is an ok, with the partition and offset the
// message was stored at. A kafka.Error on the channel is the client's own,
// not one message's, and is recorded as a client-error; a fatal one means the
// producer can't go on.
func (wr *writer) reports() {
	defer close(wr.done)
	for ev := range wr.p.Events() {
		switch e := ev.(type) {
		case *kafka.Message:
			v, _ := e.Opaque.(uint64)
			if e.TopicPartition.Error != nil {
				wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: e.TopicPartition.Error.Error()})
				continue
			}
			p, o := e.TopicPartition.Partition, int64(e.TopicPartition.Offset)
			wr.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &p, Offset: &o})
		case kafka.Error:
			msg := e.Error()
			if e.IsFatal() {
				msg = "fatal: " + msg
			}
			wr.rec.op(op{Process: "producer", Type: "info", F: "client-error", Error: msg})
		}
	}
}

// close waits until every message is delivered or given up on, then closes
// the producer, which closes the Events channel once the last report is on
// it, and waits for reports to record them all. librdkafka gives up on a
// message after message.timeout.ms, 5 minutes by default, so Flush gets a
// little longer than that; anything still outstanding then is recorded as a
// client-error.
func (wr *writer) close() {
	if left := wr.p.Flush(int((5*time.Minute + 30*time.Second).Milliseconds())); left > 0 {
		wr.rec.op(op{Process: "producer", Type: "info", F: "client-error", Error: fmt.Sprintf("%d messages or reports still outstanding after 5.5 minutes of flushing", left)})
	}
	wr.p.Close()
	<-wr.done
}

// helper is the solo consumer: a confluent-kafka-go Consumer assigned every
// partition from the first offset. confluent-kafka-go won't make a consumer
// without a group.id, but an assigned consumer never joins the group, and
// with enable.auto.commit off it commits nothing. librdkafka fetches every
// partition in the background, and one goroutine polls for what it fetched.
type helper struct {
	c    *kafka.Consumer
	quit chan struct{} // closed to end the polling loop
	done chan struct{} // closed once the polling loop has ended

	mu      sync.Mutex
	reached map[int32]int64 // partition -> the highest offset delivered
}

// startHelper starts the helper. It records subscribe around making the
// consumer and assigning it the partitions.
func startHelper(brokers string, rec *recorder, topic string, partitions int32) (*helper, error) {
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "driftlab-helper-" + topic,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}
	tps := make([]kafka.TopicPartition, partitions)
	for p := range tps {
		tps[p] = kafka.TopicPartition{Topic: &topic, Partition: int32(p), Offset: kafka.OffsetBeginning}
	}
	if err := c.Assign(tps); err != nil {
		c.Close()
		return nil, err
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	h := &helper{c: c, quit: make(chan struct{}), done: make(chan struct{}), reached: map[int32]int64{}}
	go h.deliver(rec)
	return h, nil
}

// deliver records every message the consumer hands over, polling a tenth of
// a second at a time, until quit is closed. An error, on its own or carried
// by a message, is recorded as a client-error; librdkafka keeps fetching on
// its own, so the loop goes on.
func (h *helper) deliver(rec *recorder) {
	defer close(h.done)
	for {
		select {
		case <-h.quit:
			return
		default:
		}
		switch e := h.c.Poll(100).(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: e.TopicPartition.Error.Error()})
				continue
			}
			p, o := e.TopicPartition.Partition, int64(e.TopicPartition.Offset)
			v, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: fmt.Sprintf("partition %d offset %d holds %q, not a driftlab value", p, o, e.Value)})
				continue
			}
			rec.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: v, Partition: &p, Offset: &o})
			h.mu.Lock()
			if got, ok := h.reached[p]; !ok || o > got {
				h.reached[p] = o
			}
			h.mu.Unlock()
		case kafka.Error:
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: e.Error()})
		}
	}
}

// waitForEnd waits until the helper has delivered every partition up to
// where the witness says it ends, or until grace runs out. It waits on the
// log, not on acknowledgements: the checker holds the helper to everything
// the final read finds, acknowledged or not. It asks again once the helper
// is there, and returns only when the ends haven't moved: with acks one, close
// can return before every copy has the last message, and until they do, the
// end a consumer sees falls short of it. It only decides when the final read
// starts; whether the helper got there is the checker's call.
func (h *helper) waitForEnd(ctx context.Context, cl *kgo.Client, topic string, partitions int32, grace time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	var prev map[int32]int64 // the ends the helper was last found to have reached
	for {
		ends, err := endOffsets(ctx, cl, topic, partitions)
		if err == nil && h.reachedEnds(ends) {
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

// reachedEnds says whether the helper has delivered the last offset of every
// partition, the one before its end. An empty partition has nothing to
// deliver.
func (h *helper) reachedEnds(ends map[int32]int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for p, end := range ends {
		if got, ok := h.reached[p]; end > 0 && (!ok || got < end-1) {
			return false
		}
	}
	return true
}

// stop ends the polling loop, waits for it, and closes the consumer, so
// every delivery it records comes before the final read.
func (h *helper) stop() {
	close(h.quit)
	<-h.done
	h.c.Close()
}

// waitForBrokers waits until the cluster lists at least want brokers as
// alive. The metadata request asks about no topics, only brokers, and a
// broker that is down or fenced isn't listed.
func waitForBrokers(ctx context.Context, cl *kgo.Client, want int) error {
	for {
		req := kmsg.NewPtrMetadataRequest()
		req.Topics = []kmsg.MetadataRequestTopic{} // empty, not nil: no topics
		resp, err := req.RequestWith(ctx, cl)
		if err == nil && len(resp.Brokers) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the cluster never listed %d live brokers: %w (is it up? docker compose -f deploy/kafka.yaml up -d)", want, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
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
	Type     string `json:"type"`
	Driver   string `json:"driver"`
	Client   string `json:"client"`
	API      string `json:"api,omitempty"`
	Server   string `json:"server"`
	Scenario string `json:"scenario"`
	Params   params `json:"params"`
	Driftlab string `json:"driftlab"`
}

type params struct {
	N          uint64 `json:"n"`
	Partitions int    `json:"partitions"`
	Topic      string `json:"topic"`
	Interval   string `json:"interval"`
	Settings   string `json:"settings"`
	Acks       int16  `json:"acks"`
	Idempotent bool   `json:"idempotent"`
	Librdkafka string `json:"librdkafka"`
	Witness    string `json:"witness"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards, the producer's settings, with the
// acknowledgements they ask for and whether they are idempotent, the
// librdkafka linked into the binary, and the franz-go the witness used. A run
// with a helper also says how it consumes: "direct" means partitions assigned
// by the driver, without joining a group.
func newMeta(c config, sc scenario, topic string) metaLine {
	client, witness, rev := provenance()
	acks := int16(-1) // every in-sync copy: librdkafka's default, and what idempotence needs
	if c.settings == "acks-one" {
		acks = 1
	}
	_, lib := kafka.LibraryVersion()
	m := metaLine{
		Type:     "meta",
		Driver:   "confluent-kafka-go",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params: params{
			N:          sc.n,
			Partitions: sc.partitions,
			Topic:      topic,
			Interval:   c.interval.String(),
			Settings:   c.settings,
			Acks:       acks,
			Idempotent: c.settings == "idempotent",
			Librdkafka: lib,
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
// writer's and the helper's goroutines can record too.
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

// provenance returns the confluent-kafka-go version linked into this
// binary, the franz-go version its witness uses, and the driftlab commit it
// was built from, so every history says what produced it.
func provenance() (client, witness, rev string) {
	client, witness, rev = "unknown", "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return client, witness, rev
	}
	for _, dep := range info.Deps {
		switch dep.Path {
		case "github.com/confluentinc/confluent-kafka-go/v2":
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
