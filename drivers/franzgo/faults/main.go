// Command franzfaults runs one of driftlab's fault-table scenarios against
// Kafka through franz-go and writes its history to stdout, in the format
// DESIGN.md specifies. It runs the same rows as the other Kafka drivers,
// with franz-go's default producer settings, which are already its
// strongest: idempotent, with acknowledgements from every in-sync copy.
// drivers/franzgo keeps the scenarios of franz-go's calibration, whose writer
// waits for each send; this one writes asynchronously, as the other drivers
// do, so every client meets the same stream.
//
// franz-go does the writing and, in the scenarios with a helper, the reading
// while the topic is written. It is also the witness, as in drivers/franzgo,
// through clients of its own: creating the topic, finding the brokers to
// fault and watching the in-sync set, finding where each partition ends, and
// the final read. The admin requests go through kmsg, as in drivers/franzgo.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"runtime/debug"
	"slices"
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
// the topic through franz-go while it is being written. A non-zero partitions
// or n overrides the -partitions or -n flag, and a non-zero interval replaces
// the pause after each value, 10 milliseconds otherwise. A fault, if set,
// starts just before value faultAt is handed over and lasts hold: "cut" stops
// brokers holding partition 0's other copies from fetching from it, "pause"
// freezes them, and "kill" kills the broker that leads partition 0 and then
// starts it again. A cut or a freeze hits the first followers of them, or all
// of them when followers is 0. A fault always starts live, on a goroutine of
// its own, so the writer goes on while docker acts on the broker, as in
// drivers/kafkago.
type scenario struct {
	consumer   bool
	partitions int
	n          uint64
	interval   time.Duration
	fault      string
	followers  int
	faultAt    uint64
	hold       time.Duration
}

// publish-only writes the topic and reads it back with the witness alone;
// control adds the helper.
//
// leader-kill-live, followers-pause, follower-pause and follower-cut are the
// scenarios of those names in drivers/sarama, drivers/kafkago and
// drivers/confluent, with the same numbers, each on a topic with one
// partition and the helper reading. leader-kill-live kills the leader just
// before value 501 of 20,000, written a millisecond apart, and starts it again
// 20 seconds later; the controller picks a new leader after the brokers'
// 9-second session timeout. franz-go retries a record without limit, and asks
// for fresh metadata when a write fails, at most every 5 seconds, so it should
// find the new leader within 5 seconds of the election. Meanwhile records pile
// up, and Produce waits while franz-go's buffer is full: 10,000 records up to
// v1.22.0, about 10 seconds of this stream, so the writer can pause too, and
// 50,000 from v1.22.1, more than the run writes.
//
// leader-kill-steady, also in those three drivers, is the same crash just
// before value 5,001, about 6 seconds in. leader-kill-live's comes about 0.6
// seconds after the topic is created, often before a follower has copied
// anything, so nothing has been acknowledged yet: the leader can't acknowledge
// what only it holds. By value 5,001 the followers have long been copying, so
// values are acknowledged before the crash, and none of them may be lost.
//
// followers-pause freezes both brokers that hold the partition's other copies
// for 25 seconds. follower-pause freezes one of them for 25 seconds, and the
// controller fences it after 9 to 12 seconds. follower-cut cuts one of them
// off from the leader for 50 seconds, and the leader drops it 30 to 45
// seconds in. franz-go asks the leader to wait up to 10 seconds for every
// in-sync copy, so in each of them a request can be answered
// REQUEST_TIMED_OUT and sent again. As an idempotent producer, franz-go sends
// it with the same sequence numbers, and the leader should recognise it and
// keep one copy.
var scenarios = map[string]scenario{
	"publish-only":       {},
	"control":            {consumer: true},
	"leader-kill-live":   {consumer: true, partitions: 1, n: 20000, interval: time.Millisecond, fault: "kill", faultAt: 501, hold: 20 * time.Second},
	"leader-kill-steady": {consumer: true, partitions: 1, n: 20000, interval: time.Millisecond, fault: "kill", faultAt: 5001, hold: 20 * time.Second},
	"followers-pause":    {consumer: true, partitions: 1, n: 4000, fault: "pause", faultAt: 501, hold: 25 * time.Second},
	"follower-pause":     {consumer: true, partitions: 1, n: 4000, fault: "pause", followers: 1, faultAt: 501, hold: 25 * time.Second},
	"follower-cut":       {consumer: true, partitions: 1, n: 6500, fault: "cut", followers: 1, faultAt: 501, hold: 50 * time.Second},
}

type config struct {
	brokers    string
	server     string
	scenario   string
	nftImage   string
	cutPort    int
	n          uint64
	partitions int
	interval   time.Duration
	grace      time.Duration

	// Settings of the helper's client alone, for experiments on how it reads.
	helperMinAge time.Duration
	helperLog    bool
}

func main() {
	var c config
	flag.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only, control, leader-kill-live, leader-kill-steady, followers-pause, follower-pause or follower-cut")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish, unless the scenario sets it")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	flag.DurationVar(&c.interval, "interval", 0, "the pause after handing each value to the producer; 0 keeps the scenario's own, or 10ms")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.StringVar(&c.nftImage, "nft-image", "driftlab-nft", "the image the cut runs nft from, inside a broker's network (docker build -t driftlab-nft deploy/nft)")
	flag.IntVar(&c.cutPort, "cut-port", 19092, "the port the brokers fetch from each other on, which the cut drops (deploy/kafka.yaml's INTERNAL listener)")
	flag.DurationVar(&c.helperMinAge, "helper-metadata-min-age", 0, "the least time between the helper's metadata refreshes, franz-go's MetadataMinAge; 0 keeps franz-go's default, 5s")
	flag.BoolVar(&c.helperLog, "helper-log", false, "write the helper's franz-go log, at info level, to stderr, each line led by the seconds since the history began")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "franzfaults:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
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
	if c.helperMinAge < 0 {
		return errors.New("-helper-metadata-min-age can't be negative")
	}
	if !sc.consumer && (c.helperMinAge > 0 || c.helperLog) {
		return fmt.Errorf("scenario %s has no helper for -helper-metadata-min-age or -helper-log to change", c.scenario)
	}
	partitions := int32(sc.partitions)
	rec := newRecorder(out)
	ctx := context.Background()

	// The witness's client, separate from the writer's and the helper's. It
	// creates the topic, waits for its leaders, finds the brokers to fault and
	// where each partition ends, and does the final read.
	seeds := strings.Split(c.brokers, ",")
	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer adm.Close()

	topic := fmt.Sprintf("driftlab-franzfaults-%s-%d", c.scenario, time.Now().UnixMilli())
	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// A broker that an earlier run froze and never thawed, because the run
	// was stopped mid-freeze, doesn't answer, so a freeze run thaws any first.
	if sc.fault == "pause" {
		thawAll()
	}
	// A broker that an earlier run killed needs a few seconds after its
	// restart to register again, and the topic can't get its copies without
	// it.
	if err := waitForBrokers(setup, adm, replicas); err != nil {
		return err
	}
	if err := createTopic(setup, adm, topic, partitions); err != nil {
		return fmt.Errorf("%w (is the cluster up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	if err := waitForLeaders(setup, adm, topic, partitions); err != nil {
		return err
	}
	// A cut that an earlier run left behind would stall this run before its
	// own fault starts, so a cut run clears any first.
	if sc.fault == "cut" {
		clearCuts(setup, adm, topic, c)
	}

	rec.writeMeta(newMeta(c, sc, topic))
	var h *helper
	if sc.consumer {
		if h, err = startHelper(seeds, rec, topic, partitions, c.helperMinAge, c.helperLog); err != nil {
			return err
		}
	}
	w, err := startWriter(seeds, rec)
	if err != nil {
		if h != nil {
			h.stop()
		}
		return err
	}
	var ft *activeFault
	for v := uint64(1); v <= sc.n; v++ {
		// The fault starts before this value is handed over and ends on its
		// own timer, because the stream goes on meanwhile.
		if sc.fault != "" && v == sc.faultAt {
			ft = startLive(func() (*activeFault, error) {
				return startFault(ctx, adm, rec, topic, sc, c)
			})
		}
		w.publish(ctx, topic, partitions, v)
		time.Sleep(c.interval)
	}
	w.close()
	if ft != nil {
		<-ft.over
		if ft.err != nil {
			return ft.err
		}
	}
	if h != nil {
		h.waitForEnd(ctx, adm, topic, partitions, c.grace)
		h.stop()
	}
	return finalRead(ctx, adm, seeds, rec, topic, partitions)
}

// writer publishes the run's values through a franz-go client of its own,
// with franz-go's default producer settings: idempotent, acknowledged by
// every in-sync copy, retried without limit, batched after at most 10
// milliseconds. Produce only buffers a record; franz-go sends it in the
// background and calls the record's promise with its position or its error,
// which records the outcome. Once the buffer is full, Produce waits for room:
// franz-go's default buffer holds 10,000 records up to v1.22.0 and 50,000 from
// v1.22.1.
type writer struct {
	cl  *kgo.Client
	rec *recorder
	wg  sync.WaitGroup // one count per record whose promise hasn't run yet
}

// startWriter makes the writer's client. The manual partitioner sends each
// record to the partition it names.
func startWriter(seeds []string, rec *recorder) (*writer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	return &writer{cl: cl, rec: rec}, nil
}

// publish records one publish and hands the value to franz-go for partition
// (v-1) mod the number of partitions, so a history can be checked by hand.
// The promise records an ok with the record's position, or a fail, not an
// info: franz-go fails an idempotent record only when it knows the record was
// not written, since its Produce docs call a record that has been sent "not
// safe to fail". So a failed value that turns up in the final read is a broken
// promise, which the checker reports as a phantom. A promise with no error but
// no position either would be an info, since an ok needs its position.
func (wr *writer) publish(ctx context.Context, topic string, partitions int32, v uint64) {
	wr.rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	r := &kgo.Record{
		Topic:     topic,
		Partition: int32((v - 1) % uint64(partitions)),
		Value:     []byte(strconv.FormatUint(v, 10)),
	}
	wr.wg.Add(1)
	wr.cl.Produce(ctx, r, func(r *kgo.Record, err error) {
		defer wr.wg.Done()
		switch {
		case err != nil:
			wr.rec.op(op{Process: "producer", Type: "fail", F: "publish", Value: v, Error: err.Error()})
		case r.Offset < 0:
			wr.rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: fmt.Sprintf("reported produced, with no offset (%d)", r.Offset)})
		default:
			p, o := r.Partition, r.Offset
			wr.rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, Partition: &p, Offset: &o})
		}
	})
}

// close waits until every record's promise has run, then closes the client.
func (wr *writer) close() {
	wr.wg.Wait()
	wr.cl.Close()
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
// fetching as soon as the client exists. A non-zero minAge sets the least
// time between the client's metadata refreshes, 5 seconds by default. With
// logs set, the client writes its log, at info level, to stderr, each line
// led by the seconds since the history began, the clock time_ns counts on,
// so its lines can be set against the history's.
func startHelper(seeds []string, rec *recorder, topic string, partitions int32, minAge time.Duration, logs bool) (*helper, error) {
	start := map[int32]kgo.Offset{}
	for p := int32(0); p < partitions; p++ {
		start[p] = kgo.NewOffset().AtStart()
	}
	opts := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: start})}
	if minAge > 0 {
		opts = append(opts, kgo.MetadataMinAge(minAge))
	}
	if logs {
		// A copy of the history's start, taken before the client's
		// goroutines exist, so they never touch the recorder.
		began := rec.start
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return fmt.Sprintf("%.3f ", time.Since(began).Seconds())
		})))
	}
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	cl, err := kgo.NewClient(opts...)
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

// waitForEnd waits until the helper has delivered every partition up to
// where the witness says it ends, or until grace runs out. It waits on the
// log, not on acknowledgements: the checker holds the helper to everything
// the final read finds, acknowledged or not. It asks again once the helper is
// there, and returns only when the ends haven't moved. It only decides when
// the final read starts; whether the helper got there is the checker's call.
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

// stop closes the helper's client, which ends its poll loop, and waits for
// the loop to finish, so every delivery it records comes before the final
// read.
func (h *helper) stop() {
	h.cl.Close()
	<-h.done
}

// activeFault is a fault in place. over closes once it has ended, with err
// set if something went wrong.
type activeFault struct {
	over chan struct{}
	err  error
}

// startFault puts the scenario's fault in place and returns it.
func startFault(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, sc scenario, c config) (*activeFault, error) {
	switch sc.fault {
	case "cut":
		return startCut(ctx, cl, rec, topic, sc.followers, sc.hold, c)
	case "kill":
		return startKill(ctx, cl, rec, topic, sc.hold)
	case "pause":
		return startPause(ctx, cl, rec, topic, sc.followers, sc.hold)
	}
	return nil, fmt.Errorf("unknown fault %q", sc.fault)
}

// startLive runs start on a goroutine of its own and returns at once, so the
// writer goes on while docker acts on the broker. The fault it returns ends
// when the started one does, and carries any error from starting it, which
// the run then reports once the writer is done.
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

// startCut cuts the fetches of n of the brokers that hold partition 0's other
// copies, or of all of them when n is 0, but never its leader's, and heals them
// after hold on a goroutine of its own, because the stream goes on meanwhile.
// watchISR notes when the in-sync set drops them. A cut broker stops copying
// from the leader but keeps checking in with the controller and answering
// clients, so it stays in the in-sync set until the leader drops it for
// lagging, after 30 seconds. Until then the leader stores each batch but can't
// count it as copied, and once the request's timeout runs out it answers
// REQUEST_TIMED_OUT. The cut only counts as done if nft showed the drop rule in
// every cut broker's network, and its ok records the partition's in-sync set as
// the leader saw it just before the heal. Containers are named after the
// brokers' node IDs, as deploy/kafka.yaml names them.
func startCut(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, n int, hold time.Duration, c config) (*activeFault, error) {
	leader, hit, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(hit) {
		hit = hit[:n]
	}
	names := make([]string, len(hit))
	for i, id := range hit {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "cut", Detail: map[string]any{"leader": leader, "cut": names}})
	began := time.Now()
	for _, name := range names {
		if _, err := nft(c, name, cutRules(c.cutPort), "-f", "-"); err != nil {
			healCuts(c, names) // never leave one of them cut
			return nil, fmt.Errorf("%w (is the image built? docker build -t %s deploy/nft)", err, c.nftImage)
		}
	}
	took, checkErr := cutsInPlace(c, names)
	ct := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ct.over)
		end := time.Now().Add(hold)
		watchISR(ctx, cl, rec, leader, topic, hit, began, end)
		time.Sleep(time.Until(end))
		isr, isrErr := leaderISR(ctx, cl, leader, topic)
		// Heal whatever the check said, so a run never leaves brokers cut.
		if err := healCuts(c, names); err != nil {
			ct.err = err
			return
		}
		if checkErr != nil {
			ct.err = checkErr
			return
		}
		if took {
			var detail any
			if isrErr == nil {
				detail = map[string]any{"isr": isr}
			}
			rec.op(op{Process: "nemesis", Type: "ok", F: "cut", Detail: detail})
		}
	}()
	return ct, nil
}

// cutTable is the nftables table the cut adds to a broker's network
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
// delete; a missing image shows up when the cut starts.
func clearCuts(ctx context.Context, cl *kgo.Client, topic string, c config) {
	leader, followers, err := followersOf(ctx, cl, topic)
	if err != nil {
		return
	}
	for _, id := range append(followers, leader) {
		nft(c, fmt.Sprintf("kafka-%d", id), "", "delete", "table", "inet", cutTable)
	}
}

// startPause freezes n of the brokers that hold partition 0's other copies, or
// all of them when n is 0, with docker pause, but never its leader, and thaws
// them after hold, on a goroutine of its own, because the stream goes on
// meanwhile. With two of the three brokers frozen there is also no controller
// quorum, so nothing can take them out of the in-sync set: the leader stores
// each batch but can never get it copied. With one frozen, the controller
// fences it, and watchISR notes when that drops it from the set. Unlike a cut
// broker, a frozen one doesn't answer clients either. The freeze only counts as
// done if docker showed every follower paused, and its ok records the
// partition's in-sync set as the leader saw it just before the thaw. Containers
// are named after the brokers' node IDs, as deploy/kafka.yaml names them.
func startPause(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, n int, hold time.Duration) (*activeFault, error) {
	leader, hit, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(hit) {
		hit = hit[:n]
	}
	names := make([]string, len(hit))
	for i, id := range hit {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "pause", Detail: map[string]any{"leader": leader, "paused": names}})
	began := time.Now()
	if _, err := dockerIn("", append([]string{"pause"}, names...)...); err != nil {
		dockerIn("", append([]string{"unpause"}, names...)...) // never leave one of them frozen
		return nil, err
	}
	paused, checkErr := dockerIn("", append([]string{"inspect", "-f", "{{.State.Paused}}"}, names...)...)
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		end := time.Now().Add(hold)
		watchISR(ctx, cl, rec, leader, topic, hit, began, end)
		time.Sleep(time.Until(end))
		isr, isrErr := leaderISR(ctx, cl, leader, topic)
		// Thaw them whatever the check said, so a run never leaves brokers
		// frozen.
		if _, err := dockerIn("", append([]string{"unpause"}, names...)...); err != nil {
			ft.err = err
			return
		}
		if checkErr != nil {
			ft.err = checkErr
			return
		}
		if allTrue(paused, len(names)) {
			var detail any
			if isrErr == nil {
				detail = map[string]any{"isr": isr}
			}
			rec.op(op{Process: "nemesis", Type: "ok", F: "pause", Detail: detail})
		}
	}()
	return ft, nil
}

// watchISR asks the leader every 250 milliseconds for partition 0's in-sync
// set, until none of the faulted brokers is in it or end passes, and records
// an info the first time none is, with how long after began, when the fault
// was invoked: from then on the leader counts a write as copied without them,
// so the partition takes writes again. When two followers are frozen, the
// controller quorum goes with them, and the set never shrinks.
func watchISR(ctx context.Context, cl *kgo.Client, rec *recorder, leader int32, topic string, faulted []int32, began, end time.Time) {
	for time.Now().Before(end) {
		isr, err := leaderISR(ctx, cl, leader, topic)
		if err == nil && !slices.ContainsFunc(faulted, func(id int32) bool { return slices.Contains(isr, id) }) {
			rec.op(op{Process: "nemesis", Type: "info", F: "isr", Detail: map[string]any{"isr": isr, "after": time.Since(began).Round(time.Millisecond).String()}})
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// thawAll unpauses every broker in deploy/kafka.yaml. It ignores errors,
// which mostly mean the broker wasn't paused.
func thawAll() {
	for id := 1; id <= replicas; id++ {
		dockerIn("", "unpause", fmt.Sprintf("kafka-%d", id))
	}
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

// startKill kills the broker that leads partition 0 with docker kill, the
// way a crash would, and starts it again after hold, on a goroutine of its
// own, because the stream goes on meanwhile. The kill only counts if docker
// shows the broker stopped and the cluster then gives partition 0 a new
// leader. Its ok comes when that leader shows up, the moment the partition
// can be used again, so a helper's recovery is measured from there, and it
// records how long the election took. The broker comes back as a follower,
// and the fault only ends once it is back in the in-sync set, so the next
// run starts with every broker; an info line says how long that took.
// Containers are named after the brokers' node IDs, as deploy/kafka.yaml
// names them.
func startKill(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, hold time.Duration) (*activeFault, error) {
	leader, _, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("kafka-%d", leader)
	rec.op(op{Process: "nemesis", Type: "invoke", F: "kill", Detail: map[string]any{"leader": leader, "killed": name}})
	killed := time.Now()
	if _, err := dockerIn("", "kill", name); err != nil {
		dockerIn("", "start", name) // never leave it down
		return nil, err
	}
	running, checkErr := dockerIn("", "inspect", "-f", "{{.State.Running}}", name)
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		next, after, found := waitForNewLeader(ctx, cl, topic, leader, killed, killed.Add(hold))
		if checkErr == nil && running == "false" && found {
			rec.op(op{Process: "nemesis", Type: "ok", F: "kill", Detail: map[string]any{"new_leader": next, "after": after.Round(time.Millisecond).String()}})
		}
		// Start it again whatever the checks said, so a run never leaves a
		// broker down.
		time.Sleep(time.Until(killed.Add(hold)))
		if _, err := dockerIn("", "start", name); err != nil {
			ft.err = err
			return
		}
		if checkErr != nil {
			ft.err = checkErr
			return
		}
		back := waitForISR(ctx, cl, topic, leader, time.Minute)
		rec.op(op{Process: "nemesis", Type: "info", F: "restart", Detail: map[string]any{"restarted": name, "back_in_sync_after": back}})
	}()
	return ft, nil
}

// waitForNewLeader asks every 250 milliseconds which broker leads partition
// 0, until one other than old does or deadline passes. It returns that
// leader and how long after killed it showed up.
func waitForNewLeader(ctx context.Context, cl *kgo.Client, topic string, old int32, killed, deadline time.Time) (int32, time.Duration, bool) {
	for time.Now().Before(deadline) {
		if leader, err := leaderOf(ctx, cl, topic); err == nil && leader >= 0 && leader != old {
			return leader, time.Since(killed), true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return -1, 0, false
}

// waitForISR asks every 250 milliseconds for partition 0's in-sync set,
// until it includes broker id or limit passes, and says how long that took.
func waitForISR(ctx context.Context, cl *kgo.Client, topic string, id int32, limit time.Duration) string {
	start := time.Now()
	for time.Since(start) < limit {
		if isr, err := isrOf(ctx, cl, topic); err == nil && slices.Contains(isr, id) {
			return time.Since(start).Round(time.Millisecond).String()
		}
		time.Sleep(250 * time.Millisecond)
	}
	return "not within " + limit.String()
}

// leaderOf asks which broker leads partition 0, and gives the request two
// seconds, so a broker that doesn't answer can't hold up the next try.
func leaderOf(ctx context.Context, cl *kgo.Client, topic string) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	leader, _, err := followersOf(ctx, cl, topic)
	return leader, err
}

// isrOf asks for partition 0's in-sync set, and gives the request two
// seconds.
func isrOf(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, rt := range resp.Topics {
		for _, p := range rt.Partitions {
			if p.Partition == 0 {
				return p.ISR, nil
			}
		}
	}
	return nil, fmt.Errorf("no partition 0 of %s", topic)
}

// leaderISR asks partition 0's leader directly which replicas are in its
// in-sync set, and gives the request two seconds.
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

// dockerIn runs one docker command with stdin, if not empty, as its input,
// and returns what it printed. It gives up after 30 seconds, so a docker
// daemon that hangs can't hang the run.
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
	Interval   string `json:"interval"`
	Settings   string `json:"settings"`
	Acks       int16  `json:"acks"`
	Idempotent bool   `json:"idempotent"`
	Hold       string `json:"hold,omitempty"`
	Live       bool   `json:"live,omitempty"`
	Witness    string `json:"witness"`

	// HelperMinAge is the helper's metadata min age, when a run sets it.
	HelperMinAge string `json:"helper_metadata_min_age,omitempty"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards, the producer's settings, franz-go's
// defaults, which wait for every in-sync copy and are idempotent, and the
// franz-go the witness used, the same one. A run with a helper also says how
// it consumes: "direct" means partitions assigned by the driver, with no
// group. A fault run declares its fault, which completes once it has fully
// taken hold, and says it started live, as every fault does here. If a
// declared fault leaves no evidence, the checker calls the run invalid
// instead of clean. A run that sets the helper's metadata min age records
// it, since the helper then isn't on franz-go's defaults.
func newMeta(c config, sc scenario, topic string) metaLine {
	client, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "franz-go",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params: params{
			N:          sc.n,
			Partitions: sc.partitions,
			Topic:      topic,
			Interval:   c.interval.String(),
			Settings:   "defaults",
			Acks:       -1,
			Idempotent: true,
			Witness:    client,
		},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = "direct"
	}
	if sc.fault != "" {
		m.Faults = []string{sc.fault}
		m.Params.Hold = sc.hold.String()
		m.Params.Live = true
	}
	if c.helperMinAge > 0 {
		m.Params.HelperMinAge = c.helperMinAge.String()
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
