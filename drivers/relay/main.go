// Command relaydriver tests a program that copies messages from one Kafka
// cluster to another, such as a relay or a mirror, and writes the history
// to stdout, in the format DESIGN.md specifies.
//
// The program under test is a black box. The driver starts it with a
// command it is given, writes numbered values into the source cluster, can
// freeze the target cluster while the copy runs, stops the program, and
// reads the copy back. The driver's own reads and writes use franz-go
// v1.20.7, whatever the program under test links, so the witness is never
// the thing being tested. Its admin requests go through kmsg, as in
// drivers/franzgo.
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
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// scenario describes one experiment. With pause set, the brokers holding the
// copy's other replicas are frozen halfway through the writes, for hold.
type scenario struct {
	pause bool
	hold  time.Duration
}

// relay-pause freezes the copy's two follower brokers for 25 seconds, as
// drivers/franzgo's followers-pause does: longer than a produce timeout of
// a few seconds, and shorter than the 30 seconds after which a leader drops
// a follower that has stopped fetching. relay-control is the same run
// without the freeze.
var scenarios = map[string]scenario{
	"relay-control": {},
	"relay-pause":   {pause: true, hold: 25 * time.Second},
}

type config struct {
	source   string
	target   string
	relay    string
	client   string
	relayLog string
	server   string
	scenario string
	n        uint64
	interval time.Duration
	grace    time.Duration
	hold     time.Duration
	stall    time.Duration
	stop     time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.source, "source", "127.0.0.1:59092", "the source cluster's brokers, comma separated (deploy/kafka-source.yaml)")
	flag.StringVar(&c.target, "target", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "the target cluster's brokers, comma separated (deploy/kafka.yaml)")
	flag.StringVar(&c.relay, "relay", "", "the command that starts the program under test, with its arguments separated by spaces; the driver adds the source topic and the target topic")
	flag.StringVar(&c.client, "client", "", "what the program under test is, recorded in the meta line, such as its name, commit and Kafka client")
	flag.StringVar(&c.relayLog, "relay-log", "relay.log", "the file the program's output goes to")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line")
	flag.StringVar(&c.scenario, "scenario", "relay-control", "relay-control or relay-pause")
	flag.Uint64Var(&c.n, "n", 200, "number of values to write into the source")
	flag.DurationVar(&c.interval, "interval", 20*time.Millisecond, "the pause after each write")
	flag.DurationVar(&c.grace, "grace", 60*time.Second, "how long the copy gets to catch up with the source")
	flag.DurationVar(&c.hold, "hold", 0, "how long the followers stay frozen; 0 keeps the scenario's own")
	flag.DurationVar(&c.stall, "stall", 10*time.Second, "how long the copy's end must stay put during the freeze, with the source ahead, to count as stalled")
	flag.DurationVar(&c.stop, "stop", 30*time.Second, "how long the program gets to exit after SIGTERM before it is killed")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "relaydriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	if c.n < 2 {
		return errors.New("-n must be at least 2, so the freeze can fall between two writes")
	}
	argv := strings.Fields(c.relay)
	if len(argv) == 0 {
		return errors.New("-relay is required: the command that starts the program under test")
	}
	if c.client == "" {
		return errors.New("-client is required: the history has to say what was tested")
	}
	if c.hold > 0 {
		sc.hold = c.hold
	}
	rec := newRecorder(out)
	ctx := context.Background()

	// One client per cluster. The source's writes keep franz-go's defaults,
	// acks from every in-sync replica and idempotent writes, and say which
	// partition they go to.
	srcSeeds := strings.Split(c.source, ",")
	dstSeeds := strings.Split(c.target, ",")
	src, err := kgo.NewClient(kgo.SeedBrokers(srcSeeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := kgo.NewClient(kgo.SeedBrokers(dstSeeds...))
	if err != nil {
		return err
	}
	defer dst.Close()

	// The same fresh topic on both sides, with one partition, so every value
	// goes through the frozen followers and the copy can be checked by hand.
	topic := fmt.Sprintf("driftlab-%s-%d", c.scenario, time.Now().UnixMilli())
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := createTopic(setup, src, topic, 1, 1); err != nil {
		return fmt.Errorf("source: %w (is it up? docker compose -f deploy/kafka-source.yaml up -d)", err)
	}
	if err := createTopic(setup, dst, topic, 1, 3); err != nil {
		return fmt.Errorf("target: %w (is it up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	for _, cl := range []*kgo.Client{src, dst} {
		if err := waitForLeaders(setup, cl, topic, 1); err != nil {
			return err
		}
	}

	rec.writeMeta(newMeta(c, sc, topic))
	r, err := startRelay(rec, argv, topic, c.relayLog)
	if err != nil {
		return err
	}
	// From here on, an error still stops the program, so a run never leaves
	// it running.
	fail := func(err error) error {
		r.stop(rec, c.stop)
		return err
	}

	w := &writer{cl: src, rec: rec, topic: topic}
	var pauseDone chan error
	for v := uint64(1); v <= c.n; v++ {
		// The freeze waits until the copy has caught up with the first half,
		// so the program is already copying, with its producer set up, and
		// the first value written after it has to cross the frozen brokers.
		// It ends on its own timer, because the writes go on meanwhile.
		if sc.pause && v == c.n/2+1 {
			if err := waitCaughtUp(ctx, src, dst, topic, c.grace); err != nil {
				return fail(fmt.Errorf("before the freeze: %w", err))
			}
			finish, err := startPause(ctx, src, dst, rec, topic, sc.hold, c.stall)
			if err != nil {
				return fail(err)
			}
			pauseDone = make(chan error, 1)
			go func() { pauseDone <- finish() }()
		}
		w.publish(ctx, v)
		time.Sleep(c.interval)
	}
	if pauseDone != nil {
		if err := <-pauseDone; err != nil {
			return fail(err)
		}
	}
	// Whether the copy got everything is the checker's call. This only
	// decides when the program is stopped, and says so if the copy never
	// caught up.
	if err := waitCaughtUp(ctx, src, dst, topic, c.grace); err != nil {
		rec.op(op{Process: "relay", Type: "info", F: "catch-up", Error: err.Error()})
	}
	r.stop(rec, c.stop)
	return finalRead(ctx, dst, dstSeeds, rec, topic, 1)
}

// createTopic creates the run's topic with the given number of partitions
// and replicas. Each run writes to a topic of its own, so it starts empty,
// and its evidence stays in the cluster for a second reader.
func createTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, replicas int16) error {
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

// relay is the running program under test.
type relay struct {
	cmd  *exec.Cmd
	done chan error // gets the program's exit, once it has exited
	out  *os.File
}

// startRelay starts the program under test, with the source topic and the
// target topic added to its command and its output going to logPath, and
// records the start, so the history shows when the copy began.
func startRelay(rec *recorder, argv []string, topic, logPath string) (*relay, error) {
	out, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(argv[0], append(argv[1:], topic, topic)...)
	cmd.Stdout, cmd.Stderr = out, out
	rec.op(op{Process: "relay", Type: "invoke", F: "start", Detail: map[string]any{"command": strings.Join(cmd.Args, " ")}})
	if err := cmd.Start(); err != nil {
		out.Close()
		return nil, fmt.Errorf("starting the program under test: %w", err)
	}
	rec.op(op{Process: "relay", Type: "ok", F: "start", Detail: map[string]any{"pid": cmd.Process.Pid}})
	r := &relay{cmd: cmd, done: make(chan error, 1), out: out}
	go func() { r.done <- cmd.Wait() }()
	return r, nil
}

// stop asks the program to stop the way an operator would, with SIGTERM, and
// waits up to grace for it to exit. A program still running after that is
// killed, and the history says so: a copier that can't shut down is a
// finding of its own.
func (r *relay) stop(rec *recorder, grace time.Duration) {
	defer r.out.Close()
	rec.op(op{Process: "relay", Type: "invoke", F: "stop"})
	r.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-r.done:
		rec.op(op{Process: "relay", Type: "ok", F: "stop", Detail: map[string]any{"exit": exitString(err)}})
	case <-time.After(grace):
		r.cmd.Process.Kill()
		err := <-r.done
		rec.op(op{Process: "relay", Type: "info", F: "stop", Error: fmt.Sprintf("still running %s after SIGTERM, so it was killed (%s)", grace, exitString(err))})
	}
}

// exitString describes how a program exited, the way cmd.Wait reports it.
func exitString(err error) string {
	if err == nil {
		return "exit status 0"
	}
	return err.Error()
}

// writer writes the run's values into the source.
type writer struct {
	cl    *kgo.Client
	rec   *recorder
	topic string
}

// publish writes one value into partition 0 of the source and records it,
// waiting for the ack before the next one. An error is recorded as a fail,
// for the same reason as in drivers/franzgo: franz-go fails an idempotent
// record only when it knows the record was not written.
func (w *writer) publish(ctx context.Context, v uint64) {
	w.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	r := &kgo.Record{Topic: w.topic, Partition: 0, Value: []byte(strconv.FormatUint(v, 10))}
	if err := w.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		w.rec.op(op{Process: "producer", Type: "fail", F: "publish", Value: v, Error: err.Error()})
		return
	}
	w.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &r.Partition, Offset: &r.Offset})
}

// waitCaughtUp waits until the copy holds at least as many messages as the
// source, or until grace runs out.
func waitCaughtUp(ctx context.Context, src, dst *kgo.Client, topic string, grace time.Duration) error {
	deadline := time.Now().Add(grace)
	for {
		s, errS := endOf(ctx, src, topic)
		d, errD := endOf(ctx, dst, topic)
		if errS == nil && errD == nil && d >= s {
			return nil
		}
		if time.Now().After(deadline) {
			if err := errors.Join(errS, errD); err != nil {
				return fmt.Errorf("couldn't compare the two ends after %s: %w", grace, err)
			}
			return fmt.Errorf("the copy held %d of the source's %d messages after %s", d, s, grace)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// endOf asks where partition 0 of the topic ends. Each request gets two
// seconds, so a broker that doesn't answer during a freeze can't hold up the
// caller.
func endOf(ctx context.Context, cl *kgo.Client, topic string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ends, err := endOffsets(ctx, cl, topic, 1)
	if err != nil {
		return 0, err
	}
	return ends[0], nil
}

// startPause freezes the brokers that hold the copy's other replicas, but
// not its leader, and returns finish, which unfreezes them after hold. With
// two of the three brokers frozen, the target also has no controller
// quorum, so nothing can take the frozen followers out of the in-sync set:
// its leader stores each record but can never get it copied. Both faults
// need evidence of their own. The freeze only counts if docker shows every
// follower paused, and the stall only if the copy's end then stayed put,
// with the source ahead of it, so a freeze that missed the copy makes the
// run invalid instead of clean. Containers are named after the brokers'
// node IDs, as deploy/kafka.yaml names them.
func startPause(ctx context.Context, src, dst *kgo.Client, rec *recorder, topic string, hold, stall time.Duration) (func() error, error) {
	leader, followers, err := followersOf(ctx, dst, topic)
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
	rec.op(op{Process: "nemesis", Type: "invoke", F: "stall"})
	stop := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		watchStall(ctx, src, dst, rec, topic, stall, stop)
	}()
	return func() error {
		time.Sleep(hold)
		close(stop)
		<-watched
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

// watchStall records the stall's ok once the copy's end offset has stayed
// put for stall while the source's end was ahead of it, checking every 250
// milliseconds until stop is closed. A copy whose end doesn't move while
// the source has more for it is stuck, not idle.
func watchStall(ctx context.Context, src, dst *kgo.Client, rec *recorder, topic string, stall time.Duration, stop <-chan struct{}) {
	last := int64(-1)
	since := time.Now()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		d, err := endOf(ctx, dst, topic)
		if err != nil {
			continue
		}
		if d != last {
			last, since = d, time.Now()
			continue
		}
		if time.Since(since) < stall {
			continue
		}
		s, err := endOf(ctx, src, topic)
		if err != nil || s <= d {
			continue
		}
		rec.op(op{Process: "nemesis", Type: "ok", F: "stall", Detail: map[string]any{"source_end": s, "target_end": d, "for": stall.String()}})
		return
	}
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

// finalRead reads every partition of the copy back, from its first offset
// to its end, with a fresh client of its own: no group and no committed
// offsets (DESIGN.md, v1 design). It first asks where each partition ends,
// so it knows when it has read everything.
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
		return rec.Err() // nothing was copied, so there is nothing to read
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
	Server   string   `json:"server"`
	Scenario string   `json:"scenario"`
	Relay    bool     `json:"relay"`
	Faults   []string `json:"faults,omitempty"`
	Params   params   `json:"params"`
	Driftlab string   `json:"driftlab"`
}

type params struct {
	N        uint64 `json:"n"`
	Interval string `json:"interval"`
	Topic    string `json:"topic"`
	Witness  string `json:"witness"`
	Hold     string `json:"hold,omitempty"`
}

// newMeta describes the run: what was tested, the topic, so the evidence can
// be found in both clusters afterwards, and the franz-go the driver itself
// used. "relay": true tells the checker the acks and the final read come
// from different clusters. A freeze declares two faults, the freeze itself
// and the stall it should cause in the copy.
func newMeta(c config, sc scenario, topic string) metaLine {
	witness, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "relay",
		Client:   c.client,
		Server:   c.server,
		Scenario: c.scenario,
		Relay:    true,
		Params:   params{N: c.n, Interval: c.interval.String(), Topic: topic, Witness: witness},
		Driftlab: rev,
	}
	if sc.pause {
		m.Faults = []string{"pause", "stall"}
		m.Params.Hold = sc.hold.String()
	}
	return m
}

// recorder writes the history. Its mutex guards every other field, so the
// freeze and stall goroutines can record too.
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

// provenance returns the franz-go version linked into the driver, its own
// witness, and the driftlab commit it was built from.
func provenance() (witness, rev string) {
	witness, rev = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return witness, rev
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/twmb/franz-go" {
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
	return witness, rev
}
