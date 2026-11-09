// The real cluster: the three brokers of deploy/kafka.yaml, which the runner
// reaches through franz-go, its witness, with the admin requests going
// through kmsg, as in the drivers. Everything from waitForBrokers down is
// the drivers' own code, moved here unchanged; the faults are in faults.go
// (DESIGN.md, Driver split).

package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replicas is how many copies of each partition the cluster keeps: one on
// each of the three brokers in deploy/kafka.yaml.
const replicas = 3

// kafka is the real cluster. adm is the witness's client, separate from the
// driver's: it creates the topic, waits for its leaders, finds the brokers to
// fault, watches the in-sync set, and finds where each partition ends. The
// final read makes a client of its own, from seeds. c is what the cut needs.
type kafka struct {
	seeds []string
	adm   *kgo.Client
	c     config
}

// This line compiles only if kafka has every method a cluster needs.
var _ cluster = (*kafka)(nil)

// newKafka makes the witness's client. kgo.NewClient doesn't wait for a
// broker, so a cluster that is down shows up later, in prepare.
func newKafka(seeds []string, c config) (*kafka, error) {
	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return nil, err
	}
	return &kafka{seeds: seeds, adm: adm, c: c}, nil
}

// close closes the witness's client, once the run is over.
func (k *kafka) close() {
	k.adm.Close()
}

// prepare gets the cluster ready for a run, as the start of each driver's
// run did, within the 30 seconds that run gives it.
func (k *kafka) prepare(ctx context.Context, topic string, partitions int32, kind string) error {
	// A broker that an earlier run froze and never thawed, because the run
	// was stopped mid-freeze, doesn't answer, so a freeze run thaws any first.
	if kind == "pause" {
		thawAll()
	}
	// A broker that an earlier run killed needs a few seconds after its
	// restart to register again, and the topic can't get its copies without
	// it.
	if err := waitForBrokers(ctx, k.adm, replicas); err != nil {
		return err
	}
	if err := createTopic(ctx, k.adm, topic, partitions); err != nil {
		return fmt.Errorf("%w (is the cluster up? docker compose -f deploy/kafka.yaml up -d)", err)
	}
	if err := waitForLeaders(ctx, k.adm, topic, partitions); err != nil {
		return err
	}
	// A cut that an earlier run left behind would stall this run before its
	// own fault starts, so a cut run clears any first.
	if kind == "cut" {
		clearCuts(ctx, k.adm, topic, k.c)
	}
	return nil
}

// ends asks the witness where each partition ends.
func (k *kafka) ends(ctx context.Context, topic string, partitions int32) (map[int32]int64, error) {
	return endOffsets(ctx, k.adm, topic, partitions)
}

// readBack is the final read: the witness reads every partition back, with a
// client of its own.
func (k *kafka) readBack(ctx context.Context, rec *recorder, topic string, partitions int32) error {
	return finalRead(ctx, k.adm, k.seeds, rec, topic, partitions)
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
