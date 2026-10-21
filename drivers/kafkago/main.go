// Command kafkagodriver runs one driftlab scenario against Kafka through
// segmentio/kafka-go and writes its history to stdout, in the format
// DESIGN.md specifies.
//
// kafka-go does the writing and, in the scenarios with a helper, the reading
// while the topic is written. Everything else goes through franz-go v1.20.7,
// pinned in go.mod: creating the topic, finding where each partition ends,
// and the final read, so the witness is never the thing being tested.
// franz-go's admin requests go through kmsg, as in drivers/franzgo.
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

	kafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replicas is how many copies of each partition the cluster keeps: one on
// each of the three brokers in deploy/kafka.yaml.
const replicas = 3

// scenario describes one experiment. With consumer set, a solo helper reads
// the topic through kafka-go while it is being written. A non-zero partitions
// or n overrides the -partitions or -n flag.
type scenario struct {
	consumer   bool
	partitions int
	n          uint64
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
	acks       string
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
	flag.StringVar(&c.acks, "acks", "all", "which copies acknowledge a write: all, every in-sync copy (the driver's default); one, the leader alone; or none, nothing at all (kafka-go's own default)")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish, unless the scenario sets it")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	flag.DurationVar(&c.interval, "interval", 10*time.Millisecond, "the pause after handing each value to the writer")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kafkagodriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	acks, err := requiredAcks(c.acks)
	if err != nil {
		return err
	}
	if sc.partitions == 0 {
		sc.partitions = c.partitions
	}
	if sc.n == 0 {
		sc.n = c.n
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

	// The witness's client, franz-go and never kafka-go. It creates the topic,
	// waits for its leaders, finds where each partition ends, and does the
	// final read.
	seeds := strings.Split(c.brokers, ",")
	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer adm.Close()

	topic := fmt.Sprintf("driftlab-kafkago-%s-%d", c.scenario, time.Now().UnixMilli())
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

	rec.writeMeta(newMeta(c, sc, topic, acks))
	var h *helper
	if sc.consumer {
		h = startHelper(seeds, rec, topic, partitions)
	}
	w := startWriter(seeds, rec, topic, acks)
	for v := uint64(1); v <= sc.n; v++ {
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

// requiredAcks turns -acks into kafka-go's setting. all is the strongest
// kafka-go has: it has no idempotent producer. none is what a Writer gets
// when RequiredAcks is left unset, as in every Writer in kafka-go's README;
// kafka-go's docs call it fire-and-forget.
func requiredAcks(s string) (kafka.RequiredAcks, error) {
	switch s {
	case "all":
		return kafka.RequireAll, nil
	case "one":
		return kafka.RequireOne, nil
	case "none":
		return kafka.RequireNone, nil
	}
	return 0, fmt.Errorf("-acks %q: want all, one or none", s)
}

// writer publishes the run's values through kafka-go's Writer, which runs
// asynchronously here: WriteMessages returns at once, and kafka-go reports
// what became of each value through the Completion callback, a batch at a
// time, so several values are in flight together. Every other setting stays
// at kafka-go's default: a batch goes out at 100 messages or after a second,
// whichever comes first, one batch at a time per partition, with up to 10
// attempts. Each message carries its value in WriterData, which kafka-go
// hands back to Completion untouched.
type writer struct {
	w    *kafka.Writer
	rec  *recorder
	acks kafka.RequiredAcks

	mu   sync.Mutex
	last map[int32]int64 // partition -> the highest offset acknowledged so far
}

// startWriter makes the writer. kafka-go's Writer connects on its first
// message.
func startWriter(seeds []string, rec *recorder, topic string, acks kafka.RequiredAcks) *writer {
	wr := &writer{rec: rec, acks: acks, last: map[int32]int64{}}
	wr.w = &kafka.Writer{
		Addr:         kafka.TCP(seeds...),
		Topic:        topic,
		Balancer:     byValue{},
		RequiredAcks: acks,
		Async:        true,
		Completion:   wr.completed,
	}
	return wr
}

// publish records one publish and hands the value to the writer. In
// asynchronous mode WriteMessages only returns an error when it can't take
// the message at all, before anything is sent, and the value is then
// recorded as an info at once.
func (wr *writer) publish(v uint64) {
	wr.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	m := kafka.Message{Value: []byte(strconv.FormatUint(v, 10)), WriterData: v}
	if err := wr.w.WriteMessages(context.Background(), m); err != nil {
		wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
	}
}

// completed records what became of one batch, as kafka-go reports it. When
// the batch failed, each value is an info, not a fail: kafka-go retries
// without a producer ID, so an attempt that timed out may still have been
// stored. With acks none nothing acknowledges a write, so kafka-go reports
// success with no position, and each value is an info too: whether it was
// stored is unknown. Otherwise each value is an ok, with the partition and
// offset its batch was stored at.
func (wr *writer) completed(msgs []kafka.Message, err error) {
	for _, m := range msgs {
		v, _ := m.WriterData.(uint64)
		switch {
		case err != nil:
			wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
		case wr.acks == kafka.RequireNone:
			wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: "sent with acks none, so nothing acknowledged it"})
		default:
			p, o := int32(m.Partition), m.Offset
			wr.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &p, Offset: &o})
			wr.mu.Lock()
			if got, ok := wr.last[p]; !ok || o > got {
				wr.last[p] = o
			}
			wr.mu.Unlock()
		}
	}
}

// close flushes what the writer still holds and waits for its last
// Completion calls, which Close blocks on. It returns the highest offset
// acknowledged in each partition.
func (wr *writer) close() map[int32]int64 {
	if err := wr.w.Close(); err != nil {
		wr.rec.op(op{Process: "producer", Type: "info", F: "client-error", Error: err.Error()})
	}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	return wr.last
}

// byValue is a kafka-go Balancer that sends value v to partition (v-1) mod
// the number of partitions, so a history can be checked by hand.
type byValue struct{}

func (byValue) Balance(msg kafka.Message, partitions ...int) int {
	v, _ := msg.WriterData.(uint64)
	return partitions[int((v-1)%uint64(len(partitions)))]
}

// helper is the solo consumer: a kafka-go Reader for every partition, with
// the partition set and no group, reading from the first offset. Without a
// group a Reader commits nothing. kafka-go hands messages over one at a
// time, so its deliver lines carry no poll number.
type helper struct {
	readers []*kafka.Reader
	wg      sync.WaitGroup

	mu      sync.Mutex
	reached map[int32]int64 // partition -> the highest offset delivered
}

// startHelper starts the helper. It records subscribe around making the
// readers; each one connects and starts fetching on its first FetchMessage,
// which its goroutine makes at once.
func startHelper(seeds []string, rec *recorder, topic string, partitions int32) *helper {
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	h := &helper{reached: map[int32]int64{}}
	for p := int32(0); p < partitions; p++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     seeds,
			Topic:       topic,
			Partition:   int(p),
			StartOffset: kafka.FirstOffset,
		})
		h.readers = append(h.readers, r)
		h.wg.Add(1)
		go h.deliver(rec, r)
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	return h
}

// deliver records every message one reader hands over, until the reader is
// closed, when FetchMessage returns io.EOF. Any other error is recorded, and
// the loop waits a tenth of a second before it asks again, so a reader that
// keeps failing at once can't flood the history.
func (h *helper) deliver(rec *recorder, r *kafka.Reader) {
	defer h.wg.Done()
	for {
		m, err := r.FetchMessage(context.Background())
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: err.Error()})
			time.Sleep(100 * time.Millisecond)
			continue
		}
		p, o := int32(m.Partition), m.Offset
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

// waitFor waits until the helper has delivered, in every partition, the last
// offset the writer saw acknowledged, or until grace runs out. It only
// decides when the final read starts; whether the helper got there is the
// checker's call. With acks none nothing was acknowledged, so it returns at
// once.
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

// stop closes every reader, which makes its FetchMessage return io.EOF, and
// waits until each delivery loop has ended, so every delivery it records
// comes before the final read.
func (h *helper) stop() {
	for _, r := range h.readers {
		r.Close()
	}
	h.wg.Wait()
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
	Async      bool   `json:"async"`
	Witness    string `json:"witness"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards, the acknowledgements the writer asked
// for, with "defaults" for kafka-go's own default of none, that it writes
// asynchronously, which it always does here, and the franz-go the witness
// used. A run with a helper also says how it consumes: "direct" means
// partitions assigned by the driver, with no group.
func newMeta(c config, sc scenario, topic string, acks kafka.RequiredAcks) metaLine {
	client, witness, rev := provenance()
	settings := "acks-" + c.acks
	if acks == kafka.RequireNone {
		settings = "defaults"
	}
	m := metaLine{
		Type:     "meta",
		Driver:   "kafka-go",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params: params{
			N:          sc.n,
			Partitions: sc.partitions,
			Topic:      topic,
			Interval:   c.interval.String(),
			Settings:   settings,
			Acks:       int16(acks),
			Async:      true,
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

// provenance returns the kafka-go version linked into this binary, the
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
		case "github.com/segmentio/kafka-go":
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
