// Command saramadriver runs one driftlab scenario against Kafka through
// sarama and writes its history to stdout, in the format DESIGN.md
// specifies.
//
// sarama does the writing and, in control, the reading while the topic is
// written. Everything else goes through franz-go v1.20.7, pinned in go.mod:
// creating the topic, finding which brokers to cut and where each partition
// ends, and the final read, so the witness is never the thing being tested.
// franz-go's admin requests go through kmsg, as in drivers/franzgo.
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
	"sync/atomic"
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
// the topic through sarama while it is being written. A non-zero partitions
// or n overrides the -partitions or -n flag, and a non-zero timeout replaces
// sarama's Producer.Timeout: how long the leader may wait for its copies
// before it answers REQUEST_TIMED_OUT. A non-zero maxMessages replaces
// Producer.Flush.MaxMessages, the most messages one request may carry. With
// cut set, the brokers holding partition 0's other copies stop fetching from
// it for hold, starting just before value cutAt is handed over.
type scenario struct {
	consumer    bool
	partitions  int
	n           uint64
	timeout     time.Duration
	maxMessages int
	cut         bool
	cutAt       uint64
	hold        time.Duration
}

// The calibration scenarios use one partition, so every message meets the
// cut, and a 2-second Producer.Timeout instead of sarama's 10 seconds. In
// followers-cut the leader stores each batch but can't get it copied, so it
// answers REQUEST_TIMED_OUT after 2 seconds, and once its 3 retries are used
// up, about 8 seconds in, the batch fails and sarama starts a new producer
// epoch. With the default 10 seconds the retries would outlast the cut, and
// nothing would fail. The cut holds for 20 seconds; with the few seconds nft
// takes to put it in place, that stays well under the 30 seconds after which
// the leader drops a follower that has stopped fetching. The stream goes on
// for about 10 seconds after the cut, so new messages are numbered before,
// during and after the new epochs. timeout-control is the same run without
// the cut. followers-cut-single is followers-cut with one message per
// request. Before PR 3762, a batch sent with numbers left from an earlier
// epoch is usually just turned away as out of order; a message is only lost
// when a later batch carries exactly the numbers of one the broker has just
// stored, and with one message per batch the numbers line up.
var scenarios = map[string]scenario{
	"publish-only":         {},
	"control":              {consumer: true},
	"timeout-control":      {partitions: 1, n: 3200, timeout: 2 * time.Second},
	"followers-cut":        {partitions: 1, n: 3200, timeout: 2 * time.Second, cut: true, cutAt: 201, hold: 20 * time.Second},
	"followers-cut-single": {partitions: 1, n: 3200, timeout: 2 * time.Second, maxMessages: 1, cut: true, cutAt: 201, hold: 20 * time.Second},
}

type config struct {
	brokers     string
	server      string
	scenario    string
	nftImage    string
	cutPort     int
	n           uint64
	partitions  int
	maxMessages int
	interval    time.Duration
	grace       time.Duration
	timeout     time.Duration
	hold        time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	flag.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only, control, timeout-control, followers-cut or followers-cut-single")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish, unless the scenario sets it")
	flag.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	flag.DurationVar(&c.interval, "interval", 10*time.Millisecond, "the pause after handing each value to the producer")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the helper gets to catch up before the final read")
	flag.DurationVar(&c.timeout, "timeout", 0, "sarama's Producer.Timeout; 0 keeps the scenario's own, or sarama's default")
	flag.DurationVar(&c.hold, "hold", 0, "how long the cut lasts; 0 keeps the scenario's own")
	flag.IntVar(&c.maxMessages, "max-messages", 0, "sarama's Producer.Flush.MaxMessages, the most messages one request may carry; 0 keeps the scenario's own, or sarama's default, no limit")
	flag.StringVar(&c.nftImage, "nft-image", "driftlab-nft", "the image the cut runs nft from, inside a broker's network (docker build -t driftlab-nft deploy/nft)")
	flag.IntVar(&c.cutPort, "cut-port", 19092, "the port the brokers fetch from each other on, which the cut drops (deploy/kafka.yaml's INTERNAL listener)")
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
	// The flags fill in what the scenario leaves open, and -timeout, -hold and
	// -max-messages let a run differ from its scenario, to find thresholds.
	if sc.partitions == 0 {
		sc.partitions = c.partitions
	}
	if sc.n == 0 {
		sc.n = c.n
	}
	if c.timeout > 0 {
		sc.timeout = c.timeout
	}
	if c.hold > 0 {
		sc.hold = c.hold
	}
	if c.maxMessages > 0 {
		sc.maxMessages = c.maxMessages
	}
	if sc.n < 1 {
		return errors.New("-n must be at least 1")
	}
	if sc.partitions < 1 {
		return errors.New("-partitions must be at least 1")
	}
	if c.maxMessages < 0 {
		return errors.New("-max-messages can't be negative")
	}
	partitions := int32(sc.partitions)
	rec := newRecorder(out)
	ctx := context.Background()

	// The witness's client, franz-go and never sarama. It creates the topic,
	// waits for its leaders, finds the brokers to cut and where each
	// partition ends, and does the final read.
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
	// A cut that an earlier run left behind would stall this run before its
	// own fault starts, so a cut run clears any first.
	if sc.cut {
		clearCuts(setup, adm, topic, c)
	}

	cfg := producerConfig(sc.timeout, sc.maxMessages)
	rec.writeMeta(newMeta(c, sc, topic, cfg))
	var h *helper
	if sc.consumer {
		if h, err = startHelper(seeds, rec, topic, partitions); err != nil {
			return err
		}
	}
	// In a cut run, the first publish that fails with REQUEST_TIMED_OUT after
	// the cut began is the evidence that a batch used up its retries, which
	// is when sarama starts a new epoch.
	var timedOut func()
	if sc.cut {
		timedOut = sync.OnceFunc(func() {
			rec.op(op{Process: "nemesis", Type: "ok", F: "timeout"})
		})
	}
	w, err := startWriter(seeds, cfg, rec, topic, partitions, timedOut)
	if err != nil {
		if h != nil {
			h.stop()
		}
		return err
	}
	var ct *activeCut
	for v := uint64(1); v <= sc.n; v++ {
		// The cut starts before this value is handed over and ends on its own
		// timer, because the stream goes on meanwhile.
		if sc.cut && v == sc.cutAt {
			if ct, err = startCut(ctx, adm, rec, topic, sc.hold, c); err != nil {
				return err
			}
			rec.op(op{Process: "nemesis", Type: "invoke", F: "timeout"})
			w.cutting.Store(true)
		}
		w.publish(v)
		time.Sleep(c.interval)
	}
	last := w.close()
	if ct != nil {
		<-ct.over
		if ct.err != nil {
			return ct.err
		}
	}
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
// which partition it goes to. A scenario or a flag can also change how long
// the leader may wait for its copies, timeout, and how many messages one
// request may carry, maxMessages; 0 leaves either at sarama's default.
// Everything else, the protocol version sarama speaks included, stays at
// sarama's default.
func producerConfig(timeout time.Duration, maxMessages int) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	if timeout > 0 {
		cfg.Producer.Timeout = timeout
	}
	if maxMessages > 0 {
		cfg.Producer.Flush.MaxMessages = maxMessages
	}
	return cfg
}

// writer publishes the run's values through sarama's AsyncProducer. A send
// doesn't wait for its ack, so several values can be in flight at once, as
// they are for most sarama users. Value v goes to partition (v-1) mod
// partitions, so a history can be checked by hand, and carries v in its
// Metadata, so its outcome can be matched back to it. timedOut, when set, is
// called for every REQUEST_TIMED_OUT failure once cutting is set.
type writer struct {
	p          sarama.AsyncProducer
	rec        *recorder
	topic      string
	partitions int32
	timedOut   func()
	cutting    atomic.Bool   // set once the cut has started
	done       chan struct{} // closed once every outcome has been recorded

	mu   sync.Mutex
	last map[int32]int64 // partition -> the highest offset acked so far
}

// startWriter starts the producer and the goroutine that records what
// becomes of each value.
func startWriter(seeds []string, cfg *sarama.Config, rec *recorder, topic string, partitions int32, timedOut func()) (*writer, error) {
	p, err := sarama.NewAsyncProducer(seeds, cfg)
	if err != nil {
		return nil, fmt.Errorf("starting sarama's producer: %w", err)
	}
	w := &writer{p: p, rec: rec, topic: topic, partitions: partitions, timedOut: timedOut, done: make(chan struct{}), last: map[int32]int64{}}
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
// broker never stored it, and a write that timed out may still land. In a
// cut run, a REQUEST_TIMED_OUT failure is also the timeout fault's evidence.
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
			if w.timedOut != nil && w.cutting.Load() && errors.Is(e.Err, sarama.ErrRequestTimedOut) {
				w.timedOut()
			}
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

// activeCut is a cut in place. over closes once the cut has been healed,
// with err set if something went wrong.
type activeCut struct {
	over chan struct{}
	err  error
}

// startCut cuts the fetches of the brokers that hold partition 0's other
// copies, but not its leader's, and heals them after hold on a goroutine of
// its own, because the stream goes on meanwhile. A cut broker stops copying
// from the leader but keeps checking in with the controller and answering
// clients, so it stays in the in-sync set until the leader drops it for
// lagging, after 30 seconds. Until then the leader stores each batch but
// can't count it as copied, and once Producer.Timeout runs out it answers
// REQUEST_TIMED_OUT. The cut only counts as done if nft showed the drop rule
// in every cut broker's network, and its ok records the partition's in-sync
// set as the leader saw it just before the heal. Containers are named after
// the brokers' node IDs, as deploy/kafka.yaml names them.
func startCut(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, hold time.Duration, c config) (*activeCut, error) {
	leader, followers, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(followers))
	for i, id := range followers {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "cut", Detail: map[string]any{"leader": leader, "cut": names}})
	for _, name := range names {
		if _, err := nft(c, name, cutRules(c.cutPort), "-f", "-"); err != nil {
			healCuts(c, names) // never leave one of them cut
			return nil, fmt.Errorf("%w (is the image built? docker build -t %s deploy/nft)", err, c.nftImage)
		}
	}
	took, checkErr := cutsInPlace(c, names)
	ct := &activeCut{over: make(chan struct{})}
	go func() {
		defer close(ct.over)
		time.Sleep(hold)
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
	N           uint64 `json:"n"`
	Partitions  int    `json:"partitions"`
	Topic       string `json:"topic"`
	Interval    string `json:"interval"`
	Protocol    string `json:"protocol"`
	Timeout     string `json:"timeout"`
	Retries     int    `json:"retries"`
	MaxMessages int    `json:"max_messages,omitempty"`
	Hold        string `json:"hold,omitempty"`
	Witness     string `json:"witness"`
}

// newMeta describes the run, including the topic, so the evidence can be
// found in the cluster afterwards, the Kafka protocol version sarama was told
// to speak, the producer settings a scenario can change, and the franz-go
// the witness used. A run with a helper also says how it consumes: "direct"
// means partitions assigned by the driver, with no group. A cut run declares
// the two things its trigger needs: the cut itself, and a publish that failed
// with REQUEST_TIMED_OUT once the cut began. If either leaves no evidence,
// the checker calls the run invalid instead of clean.
func newMeta(c config, sc scenario, topic string, cfg *sarama.Config) metaLine {
	client, witness, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "sarama",
		Client:   client,
		Server:   c.server,
		Scenario: c.scenario,
		Params: params{
			N:           sc.n,
			Partitions:  sc.partitions,
			Topic:       topic,
			Interval:    c.interval.String(),
			Protocol:    cfg.Version.String(),
			Timeout:     cfg.Producer.Timeout.String(),
			Retries:     cfg.Producer.Retry.Max,
			MaxMessages: cfg.Producer.Flush.MaxMessages,
			Witness:     witness,
		},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = "direct"
	}
	if sc.cut {
		m.Faults = []string{"cut", "timeout"}
		m.Params.Hold = sc.hold.String()
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
