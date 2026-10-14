// Command franzdriver runs one driftlab scenario against Kafka through
// franz-go and writes its history to stdout, in the format DESIGN.md
// specifies.
//
// The admin requests (creating the topic, finding its leaders, finding where
// each partition ends) go through kmsg, franz-go's raw protocol messages, on
// purpose. franz-go's admin package, kadm, is a separate module that requires
// a newer franz-go than the calibration pair, so importing it would quietly
// upgrade the v1.20.6 build, and the calibration would test the wrong
// library. Both v1.20.6 and v1.20.7 require kmsg v1.12.0.
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

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replicas is how many copies of each partition the cluster keeps: one on
// each of the three brokers in deploy/kafka.yaml.
const replicas = 3

// scenario describes one experiment. With consumer set, a solo helper reads
// the topic while it is being written.
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
	grace      time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only or control")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "franzdriver:", err)
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

	// The producer's client. It creates the topic, publishes and finds where
	// each partition ends. It keeps franz-go's defaults, because they are
	// what users get: acks from every in-sync replica, and idempotent writes.
	// The only change is that each record says which partition it goes to.
	seeds := strings.Split(c.brokers, ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return err
	}
	defer cl.Close()

	topic := fmt.Sprintf("driftlab-%s-%d", c.scenario, time.Now().UnixMilli())
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := createTopic(setup, cl, topic, partitions); err != nil {
		return fmt.Errorf("%w (is the cluster up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	if err := waitForLeaders(setup, cl, topic, partitions); err != nil {
		return err
	}

	rec.writeMeta(newMeta(c, sc, topic))
	var h *helper
	if sc.consumer {
		if h, err = startHelper(seeds, rec, topic, partitions); err != nil {
			return err
		}
	}
	last := map[int32]int64{} // partition -> the highest offset acked so far
	for v := uint64(1); v <= c.n; v++ {
		if p, o, ok := publish(ctx, cl, rec, topic, partitions, v); ok {
			last[p] = max(last[p], o)
		}
	}
	if h != nil {
		h.waitFor(last, c.grace)
		h.stop()
	}
	return finalRead(ctx, cl, seeds, rec, topic, partitions)
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
// a moment before every broker knows about it, and franz-go fails a record
// whose topic it still can't find after a few tries.
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

// publish records one publish and returns where its ack says it was
// stored, or false when the outcome is unknown. Value v goes to partition
// (v-1) mod partitions, so a history can be checked by hand. It waits for
// the ack before the next one, so every produce request carries a single
// record.
func publish(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, partitions int32, v uint64) (int32, int64, bool) {
	rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	r := &kgo.Record{
		Topic:     topic,
		Partition: int32((v - 1) % uint64(partitions)),
		Value:     []byte(strconv.FormatUint(v, 10)),
	}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
		return 0, 0, false
	}
	rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &r.Partition, Offset: &r.Offset})
	return r.Partition, r.Offset, true
}

// helper is the solo consumer, the Kafka counterpart of the NATS ordered
// consumer: a client of its own that reads every partition directly from
// the start, with no group and no committed offsets.
type helper struct {
	cl   *kgo.Client
	done chan struct{} // closed when the poll loop has finished

	mu      sync.Mutex
	reached map[int32]int64 // partition -> the highest offset delivered
}

// startHelper starts the helper. It records subscribe around creating the
// client, because with its partitions given up front, franz-go starts
// fetching as soon as the client exists.
func startHelper(seeds []string, rec *recorder, topic string, partitions int32) (*helper, error) {
	start := map[int32]kgo.Offset{}
	for p := int32(0); p < partitions; p++ {
		start[p] = kgo.NewOffset().AtStart()
	}
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: start}))
	if err != nil {
		return nil, err
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	h := &helper{cl: cl, done: make(chan struct{}), reached: map[int32]int64{}}
	go h.consume(rec)
	return h, nil
}

// consume polls until the client is closed. A poll hands over a batch, so
// each record becomes a deliver line of its own and carries the number of
// the poll that returned it, counted from 1 over the polls with records.
// Errors franz-go hands back are recorded as client errors.
func (h *helper) consume(rec *recorder) {
	defer close(h.done)
	var poll uint64
	for {
		fs := h.cl.PollFetches(context.Background())
		if fs.IsClientClosed() {
			return
		}
		for _, fe := range fs.Errors() {
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: fmt.Sprintf("partition %d: %v", fe.Partition, fe.Err)})
		}
		if fs.NumRecords() == 0 {
			continue
		}
		poll++
		for it := fs.RecordIter(); !it.Done(); {
			r := it.Next()
			p, o := r.Partition, r.Offset
			v, err := strconv.ParseUint(string(r.Value), 10, 64)
			if err != nil {
				rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: fmt.Sprintf("partition %d offset %d holds %q, not a driftlab value", p, o, r.Value)})
				continue
			}
			rec.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: v, Partition: &p, Offset: &o, Poll: poll})
			h.mu.Lock()
			if got, ok := h.reached[p]; !ok || o > got {
				h.reached[p] = o
			}
			h.mu.Unlock()
		}
	}
}

// waitFor waits until the helper has delivered, in every partition, the last
// offset the producer saw acked, or until grace runs out. Like the NATS
// driver's grace window, it only decides when the final read starts; whether
// the helper got there is the checker's call.
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

// stop closes the helper's client, which ends its poll loop, and waits for
// the loop to finish, so every delivery it records comes before the final
// read.
func (h *helper) stop() {
	h.cl.Close()
	<-h.done
}

// finalRead reads every partition back, from its first offset to its end.
// Kafka has no call that returns a message by its offset, so this is a
// consumer too, but a fresh one, separate from the producer, with no group
// and no committed offsets (DESIGN.md, v1 design). It first asks where each
// partition ends, so it knows when it has read everything.
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
// record a consumer can read, which, once every publish has its ack,
// includes every acked record.
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
// offset 0 are real positions, and they get written. Poll counts from 1, so
// 0 means absent there.
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
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards. A run with a helper also says how it
// consumes: "direct" means partitions assigned by the driver, with no group.
func newMeta(c config, sc scenario, topic string) metaLine {
	client, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "franzgo",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params:   params{N: c.n, Partitions: c.partitions, Topic: topic},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = "direct"
	}
	return m
}

// recorder writes the history. Its mutex guards every other field, so a
// consumer goroutine can record too.
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

// provenance returns the franz-go version linked into this binary and the
// driftlab commit it was built from, so every history says what produced it.
func provenance() (client, rev string) {
	client, rev = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return client, rev
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/twmb/franz-go" {
			client = dep.Path + "@" + dep.Version
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
	return client, rev
}
