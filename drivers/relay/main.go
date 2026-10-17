// Command relaydriver tests a program that copies messages from one Kafka
// cluster to another, such as a relay or a mirror, and writes the history
// to stdout, in the format DESIGN.md specifies.
//
// The program under test is a black box. The driver starts it with a
// command it is given and keeps it running, the way a supervisor would. It
// writes numbered values into the source cluster, can stall the target
// cluster and crash the program while the copy runs, stops the program,
// and reads the copy back. The driver's own reads and writes use franz-go
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

// scenario describes one experiment. A fault, "pause" or "cut", hits that
// many of the brokers holding the copy's other replicas, halfway through
// the writes, for hold, and the copy must then stand still for stall to
// count as stalled. "pause" freezes the brokers with docker pause; "cut"
// only drops their fetches from the other brokers, so they stay alive and
// keep checking in with the controller. With crash set, the program is
// killed during the fault and the supervisor starts it again, and once the
// copy has caught up it is restarted politely before a last batch of
// writes.
type scenario struct {
	fault     string
	followers int
	hold      time.Duration
	stall     time.Duration
	crash     bool
}

// relay-pause freezes both of the copy's follower brokers for 25 seconds,
// as drivers/franzgo's followers-pause does. With two of the three frozen
// there is no controller quorum, so nothing can take them out of the
// in-sync set, and the copy stands still for the whole freeze.
// relay-restart freezes only one, so a program started during the freeze
// can still reach the cluster. That costs time: the controller fences the
// frozen broker once its session runs out, about 9 seconds in, which takes
// it out of the in-sync set and lets the copy move again. So the stall
// only has to last 2 seconds before the driver crashes the program, and
// the new one gets a few seconds in which the copy's end is stale.
// relay-restart-cut cuts one follower's fetches instead. It keeps checking
// in, so the controller never fences it, and the leader only drops it from
// the in-sync set once it has lagged for 30 seconds, longer than the cut
// lasts, so the copy's end stays stale for the whole cut.
// relay-control is the same run without any fault.
var scenarios = map[string]scenario{
	"relay-control":     {},
	"relay-pause":       {fault: "pause", followers: 2, hold: 25 * time.Second, stall: 10 * time.Second},
	"relay-restart":     {fault: "pause", followers: 1, hold: 25 * time.Second, stall: 2 * time.Second, crash: true},
	"relay-restart-cut": {fault: "cut", followers: 1, hold: 25 * time.Second, stall: 2 * time.Second, crash: true},
}

type config struct {
	source   string
	target   string
	relay    string
	client   string
	relayLog string
	server   string
	scenario string
	nftImage string
	cutPort  int
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
	flag.StringVar(&c.relayLog, "relay-log", "relay.log", "the file the program's output goes to, from every start in the run")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line")
	flag.StringVar(&c.scenario, "scenario", "relay-control", "relay-control, relay-pause, relay-restart or relay-restart-cut")
	flag.Uint64Var(&c.n, "n", 200, "number of values to write into the source")
	flag.DurationVar(&c.interval, "interval", 20*time.Millisecond, "the pause after each write")
	flag.DurationVar(&c.grace, "grace", 60*time.Second, "how long the copy gets to catch up with the source")
	flag.DurationVar(&c.hold, "hold", 0, "how long the fault lasts; 0 keeps the scenario's own")
	flag.DurationVar(&c.stall, "stall", 0, "how long the copy's end must stay put during the fault, with the source ahead, to count as stalled; 0 keeps the scenario's own")
	flag.StringVar(&c.nftImage, "nft-image", "driftlab-nft", "the image the cut fault runs nft from, inside a broker's network (docker build -t driftlab-nft deploy/nft)")
	flag.IntVar(&c.cutPort, "cut-port", 19092, "the port the brokers fetch from each other on, which the cut fault drops (deploy/kafka.yaml's INTERNAL listener)")
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
		return errors.New("-n must be at least 2, so the fault can fall between two writes")
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
	if c.stall > 0 {
		sc.stall = c.stall
	}
	// Every program the run starts writes to the same log, one after the
	// other, so it starts empty.
	if err := os.WriteFile(c.relayLog, nil, 0o644); err != nil {
		return err
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
	// goes through the stalled followers and the copy can be checked by hand.
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
	// A cut that an earlier run left behind would stall this run's copy
	// before its own fault starts, so a cut run clears any first.
	if sc.fault == "cut" {
		clearCuts(setup, dst, topic, c)
	}

	rec.writeMeta(newMeta(c, sc, topic))
	sup, err := supervise(rec, argv, topic, c.relayLog)
	if err != nil {
		return err
	}
	// From here on, an error still stops the program, so a run never leaves
	// it running.
	fail := func(err error) error {
		sup.stop(c.stop)
		return err
	}

	w := &writer{cl: src, rec: rec, topic: topic}
	var fz *freeze
	for v := uint64(1); v <= c.n; v++ {
		// The fault waits until the copy has caught up with the first half,
		// so the program is already copying, with its producer set up, and
		// the first value written after it has to cross the stalled
		// followers. It ends on its own timer, because the writes go on
		// meanwhile.
		if sc.fault != "" && v == c.n/2+1 {
			if err := waitCaughtUp(ctx, src, dst, topic, c.grace); err != nil {
				return fail(fmt.Errorf("before the fault: %w", err))
			}
			if fz, err = startFault(ctx, src, dst, rec, topic, sc, c); err != nil {
				return fail(err)
			}
		}
		w.publish(ctx, v)
		time.Sleep(c.interval)
	}
	if fz != nil {
		// The crash waits for the stall, so the program dies with a write
		// that the copy has stored but can't count as copied yet.
		if sc.crash {
			select {
			case <-fz.stalled:
				crash(ctx, rec, sup, dst, fz, topic)
			case <-fz.over:
			}
		}
		<-fz.over
		if fz.err != nil {
			return fail(fz.err)
		}
	}
	// Whether the copy got everything is the checker's call. This only
	// decides when the program is stopped, and says so if the copy never
	// caught up.
	if err := waitCaughtUp(ctx, src, dst, topic, c.grace); err != nil {
		rec.op(op{Process: "relay", Type: "info", F: "catch-up", Error: err.Error()})
	}
	if sc.crash {
		// Restart the program once more, politely, now that the copy has
		// caught up. It starts from the copy's end, which a duplicate would
		// have pushed past the source's, so the values written next show
		// whether it skips any.
		if err := sup.stop(c.stop); err != nil {
			return err
		}
		if sup, err = supervise(rec, argv, topic, c.relayLog); err != nil {
			return err
		}
		for v := c.n + 1; v <= c.n+c.n/2; v++ {
			w.publish(ctx, v)
			time.Sleep(c.interval)
		}
		if err := waitCaughtUp(ctx, src, dst, topic, c.grace); err != nil {
			rec.op(op{Process: "relay", Type: "info", F: "catch-up", Error: err.Error()})
		}
	}
	if err := sup.stop(c.stop); err != nil {
		return err
	}
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

// relay is one start of the program under test. exited closes once it has
// exited, with exitErr set to how it did and its output file closed, so any
// number of callers can wait for it.
type relay struct {
	cmd     *exec.Cmd
	began   time.Time
	exited  chan struct{}
	exitErr error
}

// startRelay starts the program under test, with the source topic and the
// target topic added to its command and its output added to logPath, and
// records the start, so the history shows when each copy began.
func startRelay(rec *recorder, argv []string, topic, logPath string) (*relay, error) {
	out, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
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
	r := &relay{cmd: cmd, began: time.Now(), exited: make(chan struct{})}
	go func() {
		r.exitErr = cmd.Wait()
		out.Close()
		close(r.exited)
	}()
	return r, nil
}

// supervisor keeps the program under test running, the way systemd or
// Kubernetes would: whenever it exits without the driver stopping it, the
// supervisor records the exit and starts it again. A program that ran for
// a while comes back at once; one that exited within three seconds of its
// start waits a second first, so a program that can't start at all doesn't
// flood the history.
type supervisor struct {
	rec     *recorder
	argv    []string
	topic   string
	logPath string

	// mu guards the fields below it. started closes, and is replaced,
	// whenever a new program starts. err says why starting one failed, if
	// it did, and done closes once the supervisor has stopped watching.
	mu       sync.Mutex
	cur      *relay
	started  chan struct{}
	stopping bool
	err      error
	done     chan struct{}
}

// supervise starts the program and a supervisor that keeps it running.
func supervise(rec *recorder, argv []string, topic, logPath string) (*supervisor, error) {
	r, err := startRelay(rec, argv, topic, logPath)
	if err != nil {
		return nil, err
	}
	s := &supervisor{rec: rec, argv: argv, topic: topic, logPath: logPath, cur: r, started: make(chan struct{}), done: make(chan struct{})}
	go s.watch()
	return s, nil
}

// watch waits for each program to exit and starts the next, until stop.
func (s *supervisor) watch() {
	defer close(s.done)
	for {
		r, _ := s.current()
		<-r.exited
		s.mu.Lock()
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			return
		}
		s.rec.op(op{Process: "relay", Type: "info", F: "exit", Detail: map[string]any{"exit": exitString(r.exitErr)}})
		wait := 100 * time.Millisecond
		if time.Since(r.began) < 3*time.Second {
			wait = time.Second
		}
		time.Sleep(wait)
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		next, err := startRelay(s.rec, s.argv, s.topic, s.logPath)
		if err != nil {
			s.err = err
			s.mu.Unlock()
			return
		}
		s.cur = next
		close(s.started)
		s.started = make(chan struct{})
		s.mu.Unlock()
	}
}

// current returns the program the supervisor is running, or the one that
// last exited, and a channel that closes when the next one starts.
func (s *supervisor) current() (*relay, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur, s.started
}

// stop asks the program to stop the way an operator would, with SIGTERM,
// and waits up to grace for it to exit, without starting it again. A
// program still running after that is killed, and the history says so: a
// copier that can't shut down is a finding of its own. It returns the error
// that kept the supervisor from starting a program, if one did.
func (s *supervisor) stop(grace time.Duration) error {
	s.mu.Lock()
	s.stopping = true
	r := s.cur
	s.mu.Unlock()
	s.rec.op(op{Process: "relay", Type: "invoke", F: "stop"})
	r.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-r.exited:
		s.rec.op(op{Process: "relay", Type: "ok", F: "stop", Detail: map[string]any{"exit": exitString(r.exitErr)}})
	case <-time.After(grace):
		r.cmd.Process.Kill()
		<-r.exited
		s.rec.op(op{Process: "relay", Type: "info", F: "stop", Error: fmt.Sprintf("still running %s after SIGTERM, so it was killed (%s)", grace, exitString(r.exitErr))})
	}
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// exitString describes how a program exited, the way cmd.Wait reports it.
func exitString(err error) string {
	if err == nil {
		return "exit status 0"
	}
	return err.Error()
}

// crash kills the program the way a crash would, with SIGKILL, and lets the
// supervisor start it again. The crash only counts once a new program has
// run for three seconds while the copy's end still stands where it stalled.
// That shows the new program had a stale end to read, not that it read it:
// one that is slow to start can read the end after it has moved. If the
// fault ends first, the crash doesn't count.
func crash(ctx context.Context, rec *recorder, sup *supervisor, dst *kgo.Client, fz *freeze, topic string) {
	old, started := sup.current()
	rec.op(op{Process: "nemesis", Type: "invoke", F: "crash", Detail: map[string]any{"pid": old.cmd.Process.Pid}})
	old.cmd.Process.Kill()
	for {
		select {
		case <-started:
		case <-fz.over:
			return
		}
		r, next := sup.current()
		select {
		case <-r.exited:
			// It died within three seconds; wait for the supervisor's next.
			started = next
			continue
		case <-fz.over:
			return
		case <-time.After(3 * time.Second):
		}
		d, err := leaderEnd(ctx, dst, fz.leader, topic)
		select {
		case <-fz.over:
		default:
			if err == nil && d == fz.end {
				rec.op(op{Process: "nemesis", Type: "ok", F: "crash", Detail: map[string]any{"pid": r.cmd.Process.Pid, "target_end": d}})
			}
		}
		return
	}
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

// freeze is a running fault on the copy's followers, which should freeze
// the copy's end. stalled closes once the stall's ok is recorded, with end
// set to where the copy stood still, and over closes once the fault has
// ended, with err set if something went wrong.
type freeze struct {
	leader  int32
	end     int64
	stalled chan struct{}
	over    chan struct{}
	err     error
}

// startFault starts the scenario's fault on sc.followers of the brokers that
// hold the copy's other replicas, never its leader, and ends it after
// sc.hold on a goroutine of its own, because the writes go on meanwhile.
// While a follower that has stopped fetching is still in the in-sync set,
// the leader stores each record but can't count it as copied, so the copy's
// end stands still. A paused follower also stops checking in with the
// controller: with two paused there is no controller quorum, so nothing
// takes them out of the set, and with one paused, the controller fences it
// once its session runs out, about 9 seconds in, which does. A cut follower
// keeps checking in, so it stays in the set until the leader drops it for
// lagging, after 30 seconds.
//
// Both faults need evidence of their own. A pause only counts if docker
// shows every follower paused, a cut only if nft shows the drop rule in
// every follower's network, and the stall only if the copy's end then
// stayed put, with the source ahead of it, so a fault that missed the copy
// makes the run invalid instead of clean. The fault's ok also records the
// partition's in-sync set as the leader saw it just before the fault ended.
// Containers are named after the brokers' node IDs, as deploy/kafka.yaml
// names them.
func startFault(ctx context.Context, src, dst *kgo.Client, rec *recorder, topic string, sc scenario, c config) (*freeze, error) {
	leader, followers, err := followersOf(ctx, dst, topic)
	if err != nil {
		return nil, err
	}
	if sc.followers > len(followers) {
		return nil, fmt.Errorf("asked to stall %d followers of %s, which has %d", sc.followers, topic, len(followers))
	}
	names := make([]string, sc.followers)
	for i, id := range followers[:sc.followers] {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	key := "paused"
	if sc.fault == "cut" {
		key = "cut"
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: sc.fault, Detail: map[string]any{"leader": leader, key: names}})
	var took bool
	var checkErr error
	switch sc.fault {
	case "pause":
		if _, err := docker(append([]string{"pause"}, names...)...); err != nil {
			docker(append([]string{"unpause"}, names...)...) // never leave one of them frozen
			return nil, err
		}
		var paused string
		paused, checkErr = docker(append([]string{"inspect", "-f", "{{.State.Paused}}"}, names...)...)
		took = allTrue(paused, len(names))
	case "cut":
		for _, name := range names {
			if _, err := nft(c, name, cutRules(c.cutPort), "-f", "-"); err != nil {
				healCuts(c, names) // never leave one of them cut
				return nil, fmt.Errorf("%w (is the image built? docker build -t %s deploy/nft)", err, c.nftImage)
			}
		}
		took, checkErr = cutsInPlace(c, names)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "stall"})
	fz := &freeze{leader: leader, stalled: make(chan struct{}), over: make(chan struct{})}
	stop := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		watchStall(ctx, src, dst, fz, rec, topic, sc.stall, stop)
	}()
	go func() {
		defer close(fz.over)
		time.Sleep(sc.hold)
		close(stop)
		<-watched
		isr, isrErr := leaderISR(ctx, dst, leader, topic)
		// End the fault whatever the checks said, so a run never leaves
		// brokers frozen or cut.
		if err := endFault(sc.fault, names, c); err != nil {
			fz.err = err
			return
		}
		if checkErr != nil {
			fz.err = checkErr
			return
		}
		if took {
			var detail any
			if isrErr == nil {
				detail = map[string]any{"isr": isr}
			}
			rec.op(op{Process: "nemesis", Type: "ok", F: sc.fault, Detail: detail})
		}
	}()
	return fz, nil
}

// endFault ends a pause or a cut on the named brokers.
func endFault(fault string, names []string, c config) error {
	if fault == "pause" {
		_, err := docker(append([]string{"unpause"}, names...)...)
		return err
	}
	return healCuts(c, names)
}

// cutTable is the nftables table the cut fault adds to a broker's network
// namespace. Its one chain drops every packet the broker sends to the port
// the brokers fetch from each other on, so it stops copying from the
// leaders, while its traffic to the controllers and to clients carries on.
// Deleting the table ends the cut.
const cutTable = "driftlab"

// cutRules is the cut's ruleset, in the form nft -f reads.
func cutRules(port int) string {
	return fmt.Sprintf("table inet %s {\n\tchain fetches {\n\t\ttype filter hook output priority 0; policy accept;\n\t\ttcp dport %d drop\n\t}\n}\n", cutTable, port)
}

// nft runs nft inside a broker container's network namespace, from a
// throwaway container of the -nft-image image that joins that namespace with
// the right to change its firewall. stdin, if not empty, is what nft reads.
func nft(c config, name, stdin string, args ...string) (string, error) {
	full := []string{"run", "--rm", "--network", "container:" + name, "--cap-add", "NET_ADMIN"}
	if stdin != "" {
		full = append(full, "-i")
	}
	full = append(full, c.nftImage, "nft")
	return dockerIn(stdin, append(full, args...)...)
}

// cutsInPlace says whether nft shows the drop rule in every named broker's
// network.
func cutsInPlace(c config, names []string) (bool, error) {
	rule := fmt.Sprintf("tcp dport %d drop", c.cutPort)
	for _, name := range names {
		out, err := nft(c, name, "", "list", "table", "inet", cutTable)
		if err != nil {
			return false, err
		}
		if !strings.Contains(out, rule) {
			return false, nil
		}
	}
	return true, nil
}

// healCuts deletes the cut table from every named broker's network, and
// returns the first error.
func healCuts(c config, names []string) error {
	var first error
	for _, name := range names {
		if _, err := nft(c, name, "", "delete", "table", "inet", cutTable); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// clearCuts deletes any cut an earlier run left behind on the topic's
// brokers. It ignores errors, most of which mean there was no cut to
// delete; a missing image shows up when the fault starts.
func clearCuts(ctx context.Context, cl *kgo.Client, topic string, c config) {
	leader, followers, err := followersOf(ctx, cl, topic)
	if err != nil {
		return
	}
	for _, id := range append(followers, leader) {
		nft(c, fmt.Sprintf("kafka-%d", id), "", "delete", "table", "inet", cutTable)
	}
}

// watchStall records the stall's ok once the copy's end offset has stayed
// put for stall while the source's end was ahead of it, checking every 250
// milliseconds until stop is closed. A copy whose end doesn't move while
// the source has more for it is stuck, not idle. If stop comes first, it
// records what it saw instead, so a run that comes out invalid shows why.
func watchStall(ctx context.Context, src, dst *kgo.Client, fz *freeze, rec *recorder, topic string, stall time.Duration, stop <-chan struct{}) {
	last := int64(-1)
	var since time.Time
	var reads, fails int
	var lastErr string
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			rec.op(op{Process: "nemesis", Type: "info", F: "stall", Error: "the copy's end never stayed put long enough", Detail: map[string]any{"reads": reads, "failed_reads": fails, "last_error": lastErr, "target_end": last}})
			return
		case <-tick.C:
		}
		d, err := leaderEnd(ctx, dst, fz.leader, topic)
		if err != nil {
			fails++
			lastErr = err.Error()
			continue
		}
		reads++
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
		fz.end = d
		close(fz.stalled)
		return
	}
}

// leaderEnd asks the copy's leader directly where partition 0 ends. The
// usual route can first ask a broker picked at random for metadata, and
// during the freeze two of the three brokers don't answer. Asking the
// leader needs no lookup. Like endOf, it gives the request two seconds.
func leaderEnd(ctx context.Context, cl *kgo.Client, leader int32, topic string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	tp := kmsg.NewListOffsetsRequestTopicPartition()
	tp.Partition = 0
	tp.Timestamp = -1 // -1 asks for the latest offset
	t.Partitions = append(t.Partitions, tp)
	req.Topics = append(req.Topics, t)
	resp, err := cl.Broker(int(leader)).Request(ctx, req)
	if err != nil {
		return 0, err
	}
	lr, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok || len(lr.Topics) != 1 || len(lr.Topics[0].Partitions) != 1 {
		return 0, fmt.Errorf("broker %d gave an unexpected answer about where %s ends", leader, topic)
	}
	rp := lr.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(rp.ErrorCode); err != nil {
		return 0, err
	}
	return rp.Offset, nil
}

// leaderISR asks the copy's leader directly which replicas of partition 0
// are in the in-sync set, for the same reason leaderEnd asks it directly,
// and gives the request two seconds too.
func leaderISR(ctx context.Context, cl *kgo.Client, leader int32, topic string) ([]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := cl.Broker(int(leader)).Request(ctx, req)
	if err != nil {
		return nil, err
	}
	mr, ok := resp.(*kmsg.MetadataResponse)
	if !ok || len(mr.Topics) != 1 {
		return nil, fmt.Errorf("broker %d gave an unexpected answer about %s", leader, topic)
	}
	for _, p := range mr.Topics[0].Partitions {
		if p.Partition == 0 {
			return p.ISR, nil
		}
	}
	return nil, fmt.Errorf("broker %d knows no partition 0 of %s", leader, topic)
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
	return dockerIn("", args...)
}

// dockerIn is docker with stdin, if not empty, as the command's input.
func dockerIn(stdin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
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
	Stall    string `json:"stall,omitempty"`
	Frozen   int    `json:"frozen,omitempty"`
	Cut      int    `json:"cut,omitempty"`
	Again    uint64 `json:"again,omitempty"`
}

// newMeta describes the run: what was tested, the topic, so the evidence can
// be found in both clusters afterwards, and the franz-go the driver itself
// used. "relay": true tells the checker the acks and the final read come
// from different clusters. A fault declares two faults, the pause or the
// cut itself and the stall it should cause in the copy, and a crash
// scenario a third, the crash during the fault. Frozen or cut says how many
// followers the fault hit, and again how many values were written after the
// polite restart.
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
	if sc.fault != "" {
		m.Faults = []string{sc.fault, "stall"}
		m.Params.Hold = sc.hold.String()
		m.Params.Stall = sc.stall.String()
		if sc.fault == "pause" {
			m.Params.Frozen = sc.followers
		} else {
			m.Params.Cut = sc.followers
		}
	}
	if sc.crash {
		m.Faults = append(m.Faults, "crash")
		m.Params.Again = c.n / 2
	}
	return m
}

// recorder writes the history. Its mutex guards every other field, so the
// fault and stall goroutines can record too.
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
