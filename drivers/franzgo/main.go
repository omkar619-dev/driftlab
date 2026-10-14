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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
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
// the topic while it is being written. A non-zero partitions overrides the
// -partitions flag. With deadline set, every send gets a context that runs
// out after it, the way a web request gives up. With pause set, the brokers
// holding partition 0's other copies are frozen halfway through the flood,
// for hold.
type scenario struct {
	consumer   bool
	partitions int
	deadline   time.Duration
	pause      bool
	hold       time.Duration
}

// The calibration scenarios use one partition, so every send meets the
// frozen followers, and a 2-second deadline on every send. In
// followers-pause the leader stores each record but can't get it copied, so
// once franz-go's default 10-second produce timeout runs out it answers
// REQUEST_TIMED_OUT, the ambiguous answer issue 1217 needs. The freeze lasts
// 25 seconds: longer than one produce timeout, and shorter than the 30
// seconds after which a leader tries to drop a follower that has stopped
// fetching. deadline-control is the same run without the freeze.
var scenarios = map[string]scenario{
	"publish-only":     {},
	"control":          {consumer: true},
	"deadline-control": {partitions: 1, deadline: 2 * time.Second},
	"followers-pause":  {partitions: 1, deadline: 2 * time.Second, pause: true, hold: 25 * time.Second},
}

type config struct {
	brokers    string
	server     string
	scenario   string
	n          uint64
	partitions int
	grace      time.Duration
	deadline   time.Duration
	hold       time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only, control, deadline-control or followers-pause")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.DurationVar(&c.deadline, "deadline", 0, "each send's deadline; 0 keeps the scenario's own")
	flag.DurationVar(&c.hold, "hold", 0, "how long the followers stay frozen; 0 keeps the scenario's own")
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
	// -deadline and -hold let a run differ from its scenario, to find
	// thresholds. A scenario that needs one partition keeps it.
	if sc.partitions == 0 {
		sc.partitions = c.partitions
	}
	if sc.partitions < 1 {
		return errors.New("-partitions must be at least 1")
	}
	if c.deadline > 0 {
		sc.deadline = c.deadline
	}
	if c.hold > 0 {
		sc.hold = c.hold
	}
	partitions := int32(sc.partitions)
	rec := newRecorder(out)
	ctx := context.Background()

	// The producer's client. It creates the topic, publishes and finds where
	// each partition ends. It keeps franz-go's defaults, because they are
	// what users get: acks from every in-sync replica, and idempotent writes.
	// The only change is that each record says which partition it goes to.
	// In a freeze scenario it also gets a logger that watches for the
	// broker's REQUEST_TIMED_OUT.
	seeds := strings.Split(c.brokers, ",")
	opts := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	if sc.pause {
		opts = append(opts, kgo.WithLogger(timeoutWatch{seen: sync.OnceFunc(func() {
			rec.op(op{Process: "nemesis", Type: "ok", F: "timeout"})
		})}))
	}
	cl, err := kgo.NewClient(opts...)
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
	w := &writer{cl: cl, rec: rec, topic: topic, partitions: partitions, deadline: sc.deadline}
	last := map[int32]int64{} // partition -> the highest offset acked so far
	var pauseDone chan error
	for v := uint64(1); v <= c.n; v++ {
		// The freeze starts right away, before this value is sent, and ends
		// on its own timer, because the sends below keep waiting on a leader
		// that can't get its copies made. From then on, the run waits for
		// the two halves of the 1217 trigger: the broker answering
		// REQUEST_TIMED_OUT, and a send coming back after its deadline.
		if sc.pause && v == c.n/2+1 {
			finish, err := startPause(ctx, cl, rec, topic, sc.hold)
			if err != nil {
				return err
			}
			rec.op(op{Process: "nemesis", Type: "invoke", F: "timeout"})
			rec.op(op{Process: "nemesis", Type: "invoke", F: "deadline"})
			w.late = sync.OnceFunc(func() {
				rec.op(op{Process: "nemesis", Type: "ok", F: "deadline"})
			})
			pauseDone = make(chan error, 1)
			go func() { pauseDone <- finish() }()
		}
		if p, o, ok := w.publish(ctx, v); ok {
			last[p] = max(last[p], o)
		}
	}
	if pauseDone != nil {
		if err := <-pauseDone; err != nil {
			return err
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

// writer publishes the run's values. With a deadline, each send gets a
// context that runs out after it, the way a web request gives up. late, when
// set, is called whenever a send comes back after its deadline.
type writer struct {
	cl         *kgo.Client
	rec        *recorder
	topic      string
	partitions int32
	deadline   time.Duration
	late       func()
}

// publish records one publish and returns where its ack says it was stored,
// or false when it failed. Value v goes to partition (v-1) mod partitions, so
// a history can be checked by hand. It waits for the ack before the next
// one, so every produce request carries a single record.
//
// An error is recorded as a fail, not an info. franz-go fails an idempotent
// record only when it knows the record was not written: once a request has
// been sent, its Produce docs call the record "not safe to fail". So a value
// that failed but turns up in the final read is a broken promise, which the
// checker reports as a phantom.
func (w *writer) publish(ctx context.Context, v uint64) (int32, int64, bool) {
	w.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	r := &kgo.Record{
		Topic:     w.topic,
		Partition: int32((v - 1) % uint64(w.partitions)),
		Value:     []byte(strconv.FormatUint(v, 10)),
	}
	sendCtx := ctx
	if w.deadline > 0 {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, w.deadline)
		defer cancel()
	}
	err := w.cl.ProduceSync(sendCtx, r).FirstErr()
	if w.late != nil && sendCtx.Err() != nil {
		w.late()
	}
	if err != nil {
		w.rec.op(op{Process: "producer", Type: "fail", F: "publish", Value: v, Error: err.Error()})
		return 0, 0, false
	}
	w.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &r.Partition, Offset: &r.Offset})
	return r.Partition, r.Offset, true
}

// timeoutWatch is a franz-go logger that watches for one thing: a produce
// request the broker answered with REQUEST_TIMED_OUT. That answer is the
// ambiguous one 1217 needs, because the broker may have written the record
// anyway. franz-go retries a REQUEST_TIMED_OUT without an info-level line:
// its "batch in a produce request failed" is only for a batch that ends in
// an error. The answer does show up in the debug-level summary franz-go
// writes after every produce response, "produced", whose "to" value lists
// each batch, such as "retrying@50,1(REQUEST_TIMED_OUT: The request timed
// out.)". v1.20.6 and v1.20.7 write it the same way. If a later franz-go
// words it differently, this never fires and the run comes out invalid
// rather than clean.
type timeoutWatch struct {
	seen func()
}

func (t timeoutWatch) Level() kgo.LogLevel { return kgo.LogLevelDebug }

func (t timeoutWatch) Log(_ kgo.LogLevel, msg string, keyvals ...any) {
	if msg != "produced" {
		return
	}
	for i := 0; i+1 < len(keyvals); i += 2 {
		if to, ok := keyvals[i+1].(string); ok && keyvals[i] == "to" && strings.Contains(to, "REQUEST_TIMED_OUT") {
			t.seen()
		}
	}
}

// startPause freezes the brokers that hold partition 0's other copies, but
// not its leader, and returns finish, which unfreezes them after hold. With
// two of the three brokers frozen, the cluster also has no controller
// quorum, so nothing can take the frozen followers out of the in-sync set:
// the leader stores each record but can never get it copied. finish runs on
// its own goroutine, because the sends keep waiting meanwhile. The fault
// only counts as done if docker shows every follower was really paused.
// Containers are named after the brokers' node IDs, as deploy/kafka.yaml
// names them.
func startPause(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, hold time.Duration) (func() error, error) {
	leader, followers, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(followers))
	for i, id := range followers {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "pause", Detail: map[string]any{"leader": leader, "paused": names}})
	if _, err := docker(append([]string{"pause"}, names...)...); err != nil {
		docker(append([]string{"unpause"}, names...)...) // never leave one of them frozen
		return nil, err
	}
	paused, checkErr := docker(append([]string{"inspect", "-f", "{{.State.Paused}}"}, names...)...)
	return func() error {
		time.Sleep(hold)
		// Unpause whatever the check said, so a run never leaves brokers
		// frozen.
		if _, err := docker(append([]string{"unpause"}, names...)...); err != nil {
			return err
		}
		if checkErr != nil {
			return checkErr
		}
		if allTrue(paused, len(names)) {
			rec.op(op{Process: "nemesis", Type: "ok", F: "pause"})
		}
		return nil
	}, nil
}

// followersOf asks which broker leads partition 0 of the topic, and which
// brokers hold its other copies.
func followersOf(ctx context.Context, cl *kgo.Client, topic string) (int32, []int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, nil, fmt.Errorf("finding the followers of %s: %w", topic, err)
	}
	if len(resp.Topics) != 1 {
		return 0, nil, fmt.Errorf("finding the followers of %s: got %d topics back", topic, len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil {
		return 0, nil, fmt.Errorf("finding the followers of %s: %w", topic, err)
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.Partition != 0 {
			continue
		}
		var followers []int32
		for _, id := range p.Replicas {
			if id != p.Leader {
				followers = append(followers, id)
			}
		}
		return p.Leader, followers, nil
	}
	return 0, nil, fmt.Errorf("topic %s has no partition 0", topic)
}

// allTrue says whether docker inspect printed "true" once per container.
func allTrue(out string, n int) bool {
	fields := strings.Fields(out)
	if n == 0 || len(fields) != n {
		return false
	}
	for _, f := range fields {
		if f != "true" {
			return false
		}
	}
	return true
}

// docker runs one docker command and returns what it printed. It gives up
// after 30 seconds, so a docker daemon that hangs can't hang the run.
func docker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return strings.TrimSpace(string(out)), nil
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
	Deadline   string `json:"deadline,omitempty"`
	Hold       string `json:"hold,omitempty"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards. A run with a helper also says how it
// consumes: "direct" means partitions assigned by the driver, with no group.
// A freeze scenario declares the three things the 1217 trigger needs: the
// freeze itself, the broker answering REQUEST_TIMED_OUT, and a send coming
// back after its deadline. If any of them leaves no evidence, the checker
// calls the run invalid instead of clean.
func newMeta(c config, sc scenario, topic string) metaLine {
	client, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "franzgo",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params:   params{N: c.n, Partitions: sc.partitions, Topic: topic},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = "direct"
	}
	if sc.deadline > 0 {
		m.Params.Deadline = sc.deadline.String()
	}
	if sc.pause {
		m.Faults = []string{"pause", "timeout", "deadline"}
		m.Params.Hold = sc.hold.String()
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
