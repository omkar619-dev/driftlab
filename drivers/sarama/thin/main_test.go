package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// writerClosed answers close-writer.
const writerClosed = `{"event":"writer-closed"}`

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
	return driveWith(t, nil, commands...)
}

// driveWith is drive, with expect telling the mock producer, before the
// driver hands it anything, what to do with each value: one expectation per
// value, in order. The mock reports to t a value it didn't expect, and, once
// the producer is closed, an expectation no value met. A test that publishes
// ends with close-writer, so every report is written before the output is
// read.
func driveWith(t *testing.T, expect func(*mocks.AsyncProducer), commands ...string) (lines []string, producers []made, err error) {
	t.Helper()
	var stdout bytes.Buffer
	d := &driver{
		out:    &events{enc: json.NewEncoder(&stdout)},
		client: testClient,
		newProducer: func(brokers []string, cfg *sarama.Config) (sarama.AsyncProducer, error) {
			producers = append(producers, made{brokers, cfg})
			mp := mocks.NewAsyncProducer(t, cfg)
			if expect != nil {
				expect(mp)
			}
			return mp, nil
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
// reports the outcome of every value and sends each value to the partition
// the runner gave it.
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
			if !p.cfg.Producer.Return.Successes || !p.cfg.Producer.Return.Errors {
				t.Error("the producer doesn't report every outcome")
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

// eventName is the name of the event a line reports.
func eventName(t *testing.T, line string) string {
	t.Helper()
	var e struct{ Event string }
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("not JSON: %s", line)
	}
	return e.Event
}

// A publish-only run: three values over two partitions, then close-writer.
// sarama acknowledges value 1, fails value 2 with REQUEST_TIMED_OUT, and fails
// value 3 with an error that isn't Kafka's. Each command gets its one answer,
// in order. A value's report can come before or after its taken, as with any
// client that acknowledges quickly, but every report comes before
// writer-closed.
func TestWriter(t *testing.T) {
	expect := func(mp *mocks.AsyncProducer) {
		mp.ExpectInputAndSucceed()
		mp.ExpectInputAndFail(sarama.ErrRequestTimedOut)
		mp.ExpectInputAndFail(errors.New("dial tcp 127.0.0.1:29092: connect: connection refused"))
	}
	lines, _, err := driveWith(t, expect,
		strings.Replace(setupLine, `"partitions":1`, `"partitions":2`, 1),
		`{"cmd":"publish","value":1,"partition":0}`,
		`{"cmd":"publish","value":2,"partition":1}`,
		`{"cmd":"publish","value":3,"partition":0}`,
		`{"cmd":"close-writer"}`)
	if err != nil {
		t.Fatal(err)
	}
	var answers, reports []string
	for _, line := range lines {
		switch eventName(t, line) {
		case "ok", "info":
			reports = append(reports, line)
		default:
			answers = append(answers, line)
		}
	}
	wantAnswers := []string{
		readyDefaults,
		`{"event":"taken","value":1}`,
		`{"event":"taken","value":2}`,
		`{"event":"taken","value":3}`,
		writerClosed,
	}
	if !slices.Equal(answers, wantAnswers) {
		t.Errorf("answered:\n%s\nwant:\n%s", strings.Join(answers, "\n"), strings.Join(wantAnswers, "\n"))
	}
	// sarama hands successes and failures back on two channels, so the order
	// of the reports isn't fixed; sorted, they are these. The mock numbers
	// its acknowledgements from offset 1.
	slices.Sort(reports)
	wantReports := []string{
		`{"event":"info","value":2,"error":"kafka server: Request exceeded the user-specified time limit in the request","code":7}`,
		`{"event":"info","value":3,"error":"dial tcp 127.0.0.1:29092: connect: connection refused"}`,
		`{"event":"ok","value":1,"partition":0,"offset":1}`,
	}
	if !slices.Equal(reports, wantReports) {
		t.Errorf("reported:\n%s\nwant:\n%s", strings.Join(reports, "\n"), strings.Join(wantReports, "\n"))
	}
	if len(lines) == 0 || lines[len(lines)-1] != writerClosed {
		t.Errorf("the last line isn't writer-closed:\n%s", strings.Join(lines, "\n"))
	}
}

// Each value goes to sarama on the run's topic, for the runner's partition,
// with no key and the value as decimal text (rule 4).
func TestPublishMessage(t *testing.T) {
	check := func(m *sarama.ProducerMessage) error {
		value, err := m.Value.Encode()
		if err != nil {
			return err
		}
		if m.Topic != "t" || m.Partition != 0 || m.Key != nil || string(value) != "17" {
			return fmt.Errorf("got topic %q, partition %d, key %v and value %q; want t, 0, none and 17", m.Topic, m.Partition, m.Key, value)
		}
		return nil
	}
	expect := func(mp *mocks.AsyncProducer) { mp.ExpectInputWithMessageCheckerFunctionAndSucceed(check) }
	lines, _, err := driveWith(t, expect, setupLine, `{"cmd":"publish","value":17,"partition":0}`, `{"cmd":"close-writer"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(lines, `{"event":"ok","value":17,"partition":0,"offset":1}`) {
		t.Errorf("no ok for value 17:\n%s", strings.Join(lines, "\n"))
	}
}

// The writer stops, with an error, at a publish or a close-writer it can't
// carry out: a value of 0, a partition outside the topic, a publish after
// close-writer, or a second close-writer. It writes nothing for the command
// it refuses.
func TestWriterRefuses(t *testing.T) {
	closeWriter := `{"cmd":"close-writer"}`
	for _, tc := range []struct {
		name     string
		commands []string
		want     string   // what the error says
		wrote    []string // the lines written before the refusal
	}{
		{
			name:     "value 0",
			commands: []string{setupLine, `{"cmd":"publish","value":0,"partition":0}`},
			want:     `command "publish": values start at 1`,
			wrote:    []string{readyDefaults},
		},
		{
			name:     "partition outside the topic",
			commands: []string{setupLine, `{"cmd":"publish","value":1,"partition":1}`},
			want:     "partition 1, but the topic's partitions run from 0 to 0",
			wrote:    []string{readyDefaults},
		},
		{
			name:     "publish after close-writer",
			commands: []string{setupLine, closeWriter, `{"cmd":"publish","value":1,"partition":0}`},
			want:     `command "publish": came after close-writer`,
			wrote:    []string{readyDefaults, writerClosed},
		},
		{
			name:     "close-writer twice",
			commands: []string{setupLine, closeWriter, closeWriter},
			want:     `command "close-writer": came twice`,
			wrote:    []string{readyDefaults, writerClosed},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines, _, err := drive(t, tc.commands...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got error %v, want one containing %q", err, tc.want)
			}
			if !slices.Equal(lines, tc.wrote) {
				t.Errorf("wrote:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(tc.wrote, "\n"))
			}
		})
	}
}
