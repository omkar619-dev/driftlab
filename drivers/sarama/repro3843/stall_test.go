// Package repro3843 reproduces sarama issue 3843,
// https://github.com/IBM/sarama/issues/3843. When a produce request comes back
// with a retriable error, sarama's idempotent producer refreshes its metadata
// before it retries, from whichever broker LeastLoadedBroker picks. If that
// broker accepts connections but never answers, the retry waits for
// Net.ReadTimeout, although the leader would accept it at once. Below the
// package line this is the test posted in the issue, and it fails while the
// bug is there.
package repro3843

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic       = "t"
	tries       = 10
	readTimeout = 2 * time.Second
)

// TestRetryWaitsForSilentBroker sends one message per try through three mock
// brokers. Broker 3 leads the topic's only partition and times out the first
// produce request, broker 2 answers everything it is asked, and broker 1 stops
// answering just before the send. Which broker the refresh goes to is down to
// chance, so the test makes several tries and fails if any of them waited
// Net.ReadTimeout.
func TestRetryWaitsForSilentBroker(t *testing.T) {
	stalled := 0
	for i := 1; i <= tries; i++ {
		took, asked := try(t)
		t.Logf("try %2d: acked after %v, silent broker asked: %v", i, took.Round(time.Millisecond), asked)
		if took >= readTimeout {
			stalled++
		}
	}
	if stalled > 0 {
		t.Errorf("%d of %d sends waited at least Net.ReadTimeout (%v) for a broker that never answers", stalled, tries, readTimeout)
	}
}

// try makes one send and returns how long it took, and whether the producer
// sent the silent broker a request after it went silent.
func try(t *testing.T) (time.Duration, bool) {
	silent := sarama.NewMockBroker(t, 1)
	follower := sarama.NewMockBroker(t, 2)
	leader := sarama.NewMockBroker(t, 3)
	defer silent.Close()
	defer follower.Close()
	defer leader.Close()

	// Every broker hands out the same metadata, which lists the silent broker
	// first. Both this mock and LeastLoadedBroker range over maps, and Go
	// starts a small map's iteration at a random one of its 8 slots and wraps
	// around, so the broker stored first comes first about 3 times in 4. With
	// Net.MaxOpenRequests at 1, every broker reports 0 pending responses, so
	// iteration order alone decides which broker LeastLoadedBroker returns.
	metadata := sarama.NewMockMetadataResponse(t).
		SetBroker(silent.Addr(), silent.BrokerID()).
		SetBroker(follower.Addr(), follower.BrokerID()).
		SetBroker(leader.Addr(), leader.BrokerID()).
		SetLeader(topic, 0, leader.BrokerID())
	producerID := sarama.NewMockInitProducerIDResponse(t).SetProducerID(1000)
	for _, b := range []*sarama.MockBroker{silent, follower} {
		b.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest":       metadata,
			"InitProducerIDRequest": producerID,
		})
	}

	// The leader times out the first produce request, as a leader does while
	// an in-sync follower is frozen, and accepts every request after it.
	produce := sarama.NewMockSequence(
		sarama.NewMockProduceResponse(t).SetError(topic, 0, sarama.ErrRequestTimedOut),
		sarama.NewMockProduceResponse(t),
	)
	leader.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":       metadata,
		"InitProducerIDRequest": producerID,
		"ProduceRequest":        produce,
	})

	// The idempotent producer with the settings it requires (Retry.Max keeps
	// its default of 3). SyncProducer needs Return.Successes, the read timeout
	// is cut from 30 s to keep the test short, and the mock brokers don't
	// answer ApiVersions, so it is off, as in sarama's own tests.
	conf := sarama.NewConfig()
	conf.Producer.Idempotent = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Net.MaxOpenRequests = 1
	conf.Producer.Return.Successes = true
	conf.Net.ReadTimeout = readTimeout
	conf.ApiVersionsRequest = false
	p, err := sarama.NewSyncProducer([]string{leader.Addr()}, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// A mock broker with no handlers reads every request and never answers,
	// as a frozen broker does.
	silent.SetHandlerByMap(map[string]sarama.MockResponse{})
	before := len(silent.History())

	start := time.Now()
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("v")}
	if _, _, err := p.SendMessage(msg); err != nil {
		t.Fatal(err)
	}
	return time.Since(start), len(silent.History()) > before
}
