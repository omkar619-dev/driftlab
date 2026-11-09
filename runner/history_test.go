package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// ticker is a clock for the tests. It reads 0 the first time it is called,
// and a millisecond more each time after, so every line's time is known.
func ticker() func() time.Duration {
	var now time.Duration
	return func() time.Duration {
		was := now
		now += time.Millisecond
		return was
	}
}

// mustEvent reads a line a driver might write, as the runner would.
func mustEvent(t *testing.T, line string) event {
	t.Helper()
	e, err := parseEvent([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// A whole run's commands and events become exactly these lines: the meta
// line, then each operation, numbered from 0 and timed on the one clock,
// here a ticker.
func TestRecorderRun(t *testing.T) {
	var out bytes.Buffer
	r := newRecorder(&out, meta{
		API:      "direct",
		Server:   "apache/kafka:4.3.1",
		Scenario: "leader-kill-live",
		Faults:   []string{"kill"},
		Params: params{
			N:          3,
			Partitions: 2,
			Topic:      "driftlab-t",
			Interval:   "10ms",
			Settings:   "defaults",
			Options:    map[string]string{"produce_timeout": "2s"},
			Hold:       "5s",
			Live:       true,
			Witness:    "github.com/twmb/franz-go@v1.20.7",
		},
		Driftlab: "6cd08a5",
	})
	r.clock = ticker()

	r.sent(newSetup([]string{"127.0.0.1:29092"}, "driftlab-t", 2, "defaults", map[string]string{"produce_timeout": "2s"}))
	r.got(mustEvent(t, `{"event":"ready","driver":"fake","client":"fake@1","settings":{"acks":"all","enable.idempotence":"true"}}`))
	r.sent(subscribeCmd)
	r.got(mustEvent(t, `{"event":"subscribed"}`))
	r.sent(newPublish(1, 0))
	r.got(mustEvent(t, `{"event":"taken","value":1}`))
	r.got(mustEvent(t, `{"event":"ok","value":1,"partition":0,"offset":0}`))
	r.got(mustEvent(t, `{"event":"deliver","value":1,"partition":0,"offset":0,"poll":1}`))
	r.op(op{Process: "nemesis", Type: "invoke", F: "kill", Detail: map[string]any{"leader": 1}})
	r.sent(newPublish(2, 1))
	r.got(mustEvent(t, `{"event":"taken","value":2}`))
	r.got(mustEvent(t, `{"event":"info","value":2,"error":"request timed out","code":7}`))
	r.got(mustEvent(t, `{"event":"error","process":"consumer","error":"connection reset"}`))
	r.sent(newPublish(3, 0))
	r.got(mustEvent(t, `{"event":"taken","value":3}`))
	r.got(mustEvent(t, `{"event":"fail","value":3,"error":"not written"}`))
	r.sent(closeWriterCmd)
	r.got(mustEvent(t, `{"event":"writer-closed"}`))
	r.sent(stopReaderCmd)
	r.got(mustEvent(t, `{"event":"reader-stopped"}`))
	r.broke(errors.New("exited with exit status 1"))

	want := []string{
		`{"type":"meta","driver":"fake","client":"fake@1","api":"direct","server":"apache/kafka:4.3.1","scenario":"leader-kill-live","faults":["kill"],"params":{"n":3,"partitions":2,"topic":"driftlab-t","interval":"10ms","settings":"defaults","settings_in_force":{"acks":"all","enable.idempotence":"true"},"options":{"produce_timeout":"2s"},"hold":"5s","live":true,"witness":"github.com/twmb/franz-go@v1.20.7"},"driftlab":"6cd08a5"}`,
		`{"index":0,"time_ns":0,"process":"consumer","type":"invoke","f":"subscribe"}`,
		`{"index":1,"time_ns":1000000,"process":"consumer","type":"ok","f":"subscribe"}`,
		`{"index":2,"time_ns":2000000,"process":"producer","type":"invoke","f":"publish","value":1}`,
		`{"index":3,"time_ns":3000000,"process":"producer","type":"ok","f":"publish","value":1,"partition":0,"offset":0}`,
		`{"index":4,"time_ns":4000000,"process":"consumer","type":"ok","f":"deliver","value":1,"partition":0,"offset":0,"poll":1}`,
		`{"index":5,"time_ns":5000000,"process":"nemesis","type":"invoke","f":"kill","detail":{"leader":1}}`,
		`{"index":6,"time_ns":6000000,"process":"producer","type":"invoke","f":"publish","value":2}`,
		`{"index":7,"time_ns":7000000,"process":"producer","type":"info","f":"publish","value":2,"error":"request timed out","code":7}`,
		`{"index":8,"time_ns":8000000,"process":"consumer","type":"info","f":"client-error","error":"connection reset"}`,
		`{"index":9,"time_ns":9000000,"process":"producer","type":"invoke","f":"publish","value":3}`,
		`{"index":10,"time_ns":10000000,"process":"producer","type":"fail","f":"publish","value":3,"error":"not written"}`,
		`{"index":11,"time_ns":11000000,"process":"driver","type":"info","f":"broke","error":"exited with exit status 1"}`,
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(want), out.String())
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("line %d:\ngot  %s\nwant %s", i+1, lines[i], want[i])
		}
	}
}

// Nothing comes before the meta line. A driver that broke before its ready
// leaves no history, since no run began, and any other line that came first
// would be a bug in the runner, which Err reports.
func TestRecorderBeforeMeta(t *testing.T) {
	var out bytes.Buffer
	r := newRecorder(&out, meta{Scenario: "control"})
	r.broke(errors.New("exited before its stdin closed (exit status 2)"))
	if out.Len() != 0 || r.Err() != nil {
		t.Fatalf("a break before ready: wrote %q, and Err is %v; want nothing written and no error", out.String(), r.Err())
	}
	r.sent(newPublish(1, 0))
	if out.Len() != 0 {
		t.Errorf("wrote %q before the meta line", out.String())
	}
	if err := r.Err(); err == nil || !strings.Contains(err.Error(), "invoke publish from producer came before the meta line") {
		t.Errorf("Err is %v, want one saying the invoke came before the meta line", err)
	}
}

// brokenPipe is a writer whose reader has gone, so every write fails.
type brokenPipe struct{ writes int }

var errPipe = errors.New("broken pipe")

func (w *brokenPipe) Write(p []byte) (int, error) {
	w.writes++
	return 0, errPipe
}

// After a write fails, as one does once whatever reads the history has gone,
// the recorder writes nothing more, and Err returns that first error.
func TestRecorderWriteError(t *testing.T) {
	w := &brokenPipe{}
	r := newRecorder(w, meta{Scenario: "control"})
	r.got(mustEvent(t, `{"event":"ready","driver":"fake","client":"fake@1","settings":{}}`))
	r.sent(subscribeCmd)
	if w.writes != 1 {
		t.Errorf("got %d writes, want 1: none after the first failed", w.writes)
	}
	if err := r.Err(); !errors.Is(err, errPipe) {
		t.Errorf("Err is %v, want %v", err, errPipe)
	}
}

// Lines recorded from several goroutines at once, as the reader, the run
// and a fault each record, still come out whole, numbered in order with no
// gaps, and with times that never go back.
func TestRecorderConcurrent(t *testing.T) {
	var out bytes.Buffer
	r := newRecorder(&out, meta{Scenario: "control"})
	r.got(mustEvent(t, `{"event":"ready","driver":"fake","client":"fake@1","settings":{}}`))
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				r.op(op{Process: "test", Type: "info", F: "line"})
			}
		})
	}
	wg.Wait()
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 401 {
		t.Fatalf("got %d lines, want the meta line and 400 operations", len(lines))
	}
	var last int64
	for i, l := range lines[1:] {
		var o op
		if err := json.Unmarshal([]byte(l), &o); err != nil {
			t.Fatalf("line %d: %v", i+2, err)
		}
		if o.Index != i || o.TimeNS < last {
			t.Fatalf("line %d: index %d at %d ns, after %d ns; want index %d, at no earlier time", i+2, o.Index, o.TimeNS, last, i)
		}
		last = o.TimeNS
	}
}
