package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// testClient stands in for the sarama version in the tests' ready lines, so
// they stay the same whichever go.mod built the test.
const testClient = "github.com/IBM/sarama@test"

// setupLine is a setup command as the runner writes it.
const setupLine = `{"cmd":"setup","protocol":1,"brokers":["127.0.0.1:29092"],"topic":"t","partitions":1,"settings":"defaults","options":{}}`

// readyDefaults answers setupLine: sarama v1.61.1 as it ships.
const readyDefaults = `{"event":"ready","driver":"sarama","client":"github.com/IBM/sarama@test","settings":{"Net.MaxOpenRequests":"5","Producer.Flush.MaxMessages":"0","Producer.Idempotent":"false","Producer.RequiredAcks":"1","Producer.Retry.Max":"3","Producer.Timeout":"10s","Version":"2.8.0"}}`

// made is a producer the driver made: the brokers it started from, and its
// configuration.
type made struct {
	brokers []string
	cfg     *sarama.Config
}

// drive runs a driver over the commands, one per line, with sarama's mock
// producer in place of a real one; the mock reports to t any configuration
// sarama would refuse. It returns the lines the driver wrote on stdout, the
// producers it made, and how serve ended.
func drive(t *testing.T, commands ...string) (lines []string, producers []made, err error) {
	t.Helper()
	var stdout bytes.Buffer
	d := &driver{
		out:    &events{enc: json.NewEncoder(&stdout)},
		client: testClient,
		newProducer: func(brokers []string, cfg *sarama.Config) (sarama.AsyncProducer, error) {
			producers = append(producers, made{brokers, cfg})
			return mocks.NewAsyncProducer(t, cfg), nil
		},
	}
	err = d.serve(strings.NewReader(strings.Join(commands, "\n")))
	if s := stdout.String(); s != "" {
		lines = strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	}
	return lines, producers, err
}

// setup answers ready with the settings in force, for each named set and
// with both options. It makes one producer, from the runner's brokers, that
// reports every acknowledgement and sends each value to the partition the
// runner gave it.
func TestSetup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup string
		ready string
	}{
		{"defaults", setupLine, readyDefaults},
		{
			"idempotent with options",
			strings.Replace(setupLine, `"defaults","options":{}`, `"idempotent","options":{"max_batch_messages":"1","produce_timeout":"2s"}`, 1),
			`{"event":"ready","driver":"sarama","client":"github.com/IBM/sarama@test","settings":{"Net.MaxOpenRequests":"1","Producer.Flush.MaxMessages":"1","Producer.Idempotent":"true","Producer.RequiredAcks":"-1","Producer.Retry.Max":"3","Producer.Timeout":"2s","Version":"2.8.0"}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines, producers, err := drive(t, tc.setup)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(lines, []string{tc.ready}) {
				t.Fatalf("wrote:\n%s\nwant:\n%s", strings.Join(lines, "\n"), tc.ready)
			}
			if len(producers) != 1 {
				t.Fatalf("made %d producers, want 1", len(producers))
			}
			p := producers[0]
			if !slices.Equal(p.brokers, []string{"127.0.0.1:29092"}) {
				t.Errorf("made the producer from %q, want the runner's brokers", p.brokers)
			}
			if !p.cfg.Producer.Return.Successes {
				t.Error("the producer doesn't report acknowledgements")
			}
			msg := &sarama.ProducerMessage{Partition: 2}
			if got, err := p.cfg.Producer.Partitioner("t").Partition(msg, 3); err != nil || got != 2 {
				t.Errorf("a value for partition 2 went to %d (%v)", got, err)
			}
		})
	}
}

// A producer sarama can't start ends the run before ready: no meta line, so
// no run began, and the runner's stderr says why.
func TestSetupNoProducer(t *testing.T) {
	var stdout bytes.Buffer
	d := &driver{
		out:    &events{enc: json.NewEncoder(&stdout)},
		client: testClient,
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return nil, sarama.ErrOutOfBrokers
		},
	}
	err := d.serve(strings.NewReader(setupLine))
	if !errors.Is(err, sarama.ErrOutOfBrokers) || stdout.Len() > 0 {
		t.Errorf("got error %v and wrote %q, want sarama's error and nothing written", err, stdout.String())
	}
}

// The driver stops, with an error, at a command it can't carry out as asked.
// It never quietly ignores a setting or an option (rule 6), and it answers
// setup once, before anything else (rule 8). It writes nothing for the
// command it refuses.
func TestRefuses(t *testing.T) {
	setup := func(from, to string) string { return strings.Replace(setupLine, from, to, 1) }
	for _, tc := range []struct {
		name     string
		commands []string
		want     string // what the error says
		ready    bool   // ready was written before the refusal
	}{
		{"not JSON", []string{"setup"}, `reading "setup": invalid character`, false},
		{"before setup", []string{`{"cmd":"subscribe"}`}, `command "subscribe": came before setup`, false},
		{"setup twice", []string{setupLine, setupLine}, `command "setup": came twice`, true},
		{"no such command", []string{setupLine, `{"cmd":"flush"}`}, `command "flush": no such command`, true},
		{"another protocol", []string{setup(`"protocol":1`, `"protocol":2`)}, "protocol 2, but this driver speaks 1", false},
		{"no topic", []string{setup(`"topic":"t"`, `"topic":""`)}, "needs brokers, a topic and at least one partition", false},
		{"unknown settings", []string{setup(`"defaults"`, `"fast"`)}, `no settings named "fast"`, false},
		{"unknown option", []string{setup(`{}`, `{"linger_ms":"5"}`)}, `unknown option "linger_ms"`, false},
		{"timeout without a unit", []string{setup(`{}`, `{"produce_timeout":"2"}`)}, "option produce_timeout=2:", false},
		{"timeout below a millisecond", []string{setup(`{}`, `{"produce_timeout":"1500us"}`)}, "option produce_timeout=1500us:", false},
		{"no messages per request", []string{setup(`{}`, `{"max_batch_messages":"0"}`)}, "option max_batch_messages=0:", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines, _, err := drive(t, tc.commands...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got error %v, want one containing %q", err, tc.want)
			}
			var want []string
			if tc.ready {
				want = []string{readyDefaults}
			}
			if !slices.Equal(lines, want) {
				t.Errorf("wrote:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
			}
		})
	}
}

// When stdin ends, serve returns nil, so the driver exits with status 0
// (rule 8), even if no command came.
func TestStdinEnds(t *testing.T) {
	lines, producers, err := drive(t)
	if err != nil || len(lines) > 0 || len(producers) > 0 {
		t.Errorf("got error %v, wrote %q and made %d producers, want nothing at all", err, lines, len(producers))
	}
}
