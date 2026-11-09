package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCluster is a cluster for the tests. Its topic holds what stored says,
// as if the driver had written it there, and its faults only record their
// two lines. It notes what the run asks of it, in order.
type fakeCluster struct {
	stored map[int32][]uint64 // each partition's values, in offset order

	mu    sync.Mutex
	calls []string
}

func (fc *fakeCluster) note(call string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.calls = append(fc.calls, call)
}

// called returns what the run asked of the cluster, in order.
func (fc *fakeCluster) called() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return slices.Clone(fc.calls)
}

func (fc *fakeCluster) prepare(ctx context.Context, topic string, partitions int32, kind string) error {
	fc.note(fmt.Sprintf("prepare %s %d %q", topic, partitions, kind))
	return nil
}

// startFault records the fault's invoke at once, and its ok once the hold is
// over, just before the fault ends.
func (fc *fakeCluster) startFault(ctx context.Context, rec *recorder, topic string, f fault) (*activeFault, error) {
	fc.note("start " + f.kind)
	rec.op(op{Process: "nemesis", Type: "invoke", F: f.kind})
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		time.Sleep(f.hold)
		rec.op(op{Process: "nemesis", Type: "ok", F: f.kind})
		fc.note("end " + f.kind)
	}()
	return ft, nil
}

func (fc *fakeCluster) ends(ctx context.Context, topic string, partitions int32) (map[int32]int64, error) {
	fc.note("ends")
	ends := map[int32]int64{}
	for p := range partitions {
		ends[p] = int64(len(fc.stored[p]))
	}
	return ends, nil
}

func (fc *fakeCluster) readBack(ctx context.Context, rec *recorder, topic string, partitions int32) error {
	fc.note("read back")
	for p := range partitions {
		for o, v := range fc.stored[p] {
			offset := int64(o)
			rec.op(op{Process: "final-read", Type: "ok", F: "read", Value: v, Partition: &p, Offset: &offset})
		}
	}
	return nil
}

// fakeDriver is the command that starts this test program again as the fake
// driver called name, as startFake does in driver_test.go.
func fakeDriver(t *testing.T, name string) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(fakeEnv, name)
	t.Setenv("GORACE", "atexit_sleep_ms=0") // see startFake
	return []string{exe}
}

// testHold is how long the fake's faults last: long enough that a fault that
// starts on the loop ends after the run's last value, so its lines come in a
// known place.
const testHold = 300 * time.Millisecond

// testPlan is a run of one value, on one partition, with a reader and no
// fault, through the fake driver called name.
func testPlan(t *testing.T, name string) plan {
	return plan{
		scenario:   "test",
		topic:      "driftlab-test",
		n:          1,
		partitions: 1,
		interval:   time.Millisecond,
		reader:     true,
		settings:   "defaults",
		grace:      5 * time.Second,
		brokers:    []string{"127.0.0.1:29092"},
		server:     "apache/kafka:4.3.1",
		witness:    "github.com/twmb/franz-go@v1.20.7",
		driftlab:   "test",
		driver:     fakeDriver(t, name),
		deadline:   5 * time.Second,
	}
}

// stamp matches the index and the time at the front of an operation's line.
var stamp = regexp.MustCompile(`^\{"index":(\d+),"time_ns":(\d+),`)

// untimed returns a history's lines with the index and the time taken out of
// each operation, once it has checked that the indexes count up from 0 and
// that the times never go back. The lines can then be compared with what
// they should be, whatever the clock read.
func untimed(t *testing.T, history string) []string {
	t.Helper()
	if history == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(history, "\n"), "\n")
	var last int64
	for i := 1; i < len(lines); i++ {
		m := stamp.FindStringSubmatch(lines[i])
		if m == nil || m[1] != strconv.Itoa(i-1) {
			t.Fatalf("line %d doesn't start with index %d: %s", i+1, i-1, lines[i])
		}
		ns, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil || ns < last {
			t.Fatalf("line %d goes back in time: %s", i+1, lines[i])
		}
		last = ns
		lines[i] = "{" + lines[i][len(m[0]):]
	}
	return lines
}

// sameLines stops the test at the first line where got and want differ.
func sameLines(t *testing.T, got, want []string) {
	t.Helper()
	for i := range max(len(got), len(want)) {
		if i >= len(got) || i >= len(want) {
			t.Fatalf("got %d lines, want %d:\n%s", len(got), len(want), strings.Join(got, "\n"))
		}
		if got[i] != want[i] {
			t.Fatalf("line %d:\ngot  %s\nwant %s", i+1, got[i], want[i])
		}
	}
}

// A whole run, with a reader and a fault that starts on the loop, before
// value 2. The driver is the sync fake, which reports on each value before
// it answers, so every line has a known place. Each publish's invoke still
// comes before what the driver reported about it, the fault's two lines
// bracket the values sent while it held, and the run asks for the ends only
// once the fault is over.
func TestRun(t *testing.T) {
	p := testPlan(t, "sync")
	p.n, p.partitions = 3, 2
	p.options = map[string]string{"produce_timeout": "2s"}
	p.fault = fault{kind: "pause", at: 2, hold: testHold}
	fc := &fakeCluster{stored: map[int32][]uint64{0: {1, 3}, 1: {2}}}
	var out, logs bytes.Buffer
	if err := run(p, fc, &out, &logs); err != nil {
		t.Fatal(err)
	}

	want := []string{
		`{"type":"meta","driver":"fake","client":"fake@1","api":"direct","server":"apache/kafka:4.3.1","scenario":"test","faults":["pause"],"params":{"n":3,"partitions":2,"topic":"driftlab-test","interval":"1ms","settings":"defaults","settings_in_force":{"acks":"all"},"options":{"produce_timeout":"2s"},"hold":"300ms","witness":"github.com/twmb/franz-go@v1.20.7"},"driftlab":"test"}`,
		`{"process":"consumer","type":"invoke","f":"subscribe"}`,
		`{"process":"consumer","type":"ok","f":"subscribe"}`,
		`{"process":"producer","type":"invoke","f":"publish","value":1}`,
		`{"process":"producer","type":"ok","f":"publish","value":1,"partition":0,"offset":0}`,
		`{"process":"consumer","type":"ok","f":"deliver","value":1,"partition":0,"offset":0,"poll":1}`,
		`{"process":"nemesis","type":"invoke","f":"pause"}`,
		`{"process":"producer","type":"invoke","f":"publish","value":2}`,
		`{"process":"producer","type":"ok","f":"publish","value":2,"partition":1,"offset":0}`,
		`{"process":"consumer","type":"ok","f":"deliver","value":2,"partition":1,"offset":0,"poll":2}`,
		`{"process":"producer","type":"invoke","f":"publish","value":3}`,
		`{"process":"producer","type":"ok","f":"publish","value":3,"partition":0,"offset":1}`,
		`{"process":"consumer","type":"ok","f":"deliver","value":3,"partition":0,"offset":1,"poll":3}`,
		`{"process":"nemesis","type":"ok","f":"pause"}`,
		`{"process":"final-read","type":"ok","f":"read","value":1,"partition":0,"offset":0}`,
		`{"process":"final-read","type":"ok","f":"read","value":3,"partition":0,"offset":1}`,
		`{"process":"final-read","type":"ok","f":"read","value":2,"partition":1,"offset":0}`,
	}
	sameLines(t, untimed(t, out.String()), want)

	calls := []string{`prepare driftlab-test 2 "pause"`, "start pause", "end pause", "ends", "ends", "read back"}
	if got := fc.called(); !slices.Equal(got, calls) {
		t.Errorf("asked the cluster %q, want %q", got, calls)
	}
	pipe := regexp.MustCompile(`\npipe check: publish to taken took \S+ at the median and \S+ at the slowest, over 3 values\n$`)
	if !pipe.MatchString(logs.String()) {
		t.Errorf("the log doesn't end with the pipe check of 3 values:\n%s", logs.String())
	}
}

// A run with no reader and no fault: no subscribe, no wait for the reader,
// and no line from a consumer or a nemesis. The meta line names no reading
// API and no fault either.
func TestRunPublishOnly(t *testing.T) {
	p := testPlan(t, "sync")
	p.n = 2
	p.reader = false
	fc := &fakeCluster{stored: map[int32][]uint64{0: {1, 2}}}
	var out, logs bytes.Buffer
	if err := run(p, fc, &out, &logs); err != nil {
		t.Fatal(err)
	}

	want := []string{
		`{"type":"meta","driver":"fake","client":"fake@1","server":"apache/kafka:4.3.1","scenario":"test","params":{"n":2,"partitions":1,"topic":"driftlab-test","interval":"1ms","settings":"defaults","settings_in_force":{"acks":"all"},"witness":"github.com/twmb/franz-go@v1.20.7"},"driftlab":"test"}`,
		`{"process":"producer","type":"invoke","f":"publish","value":1}`,
		`{"process":"producer","type":"ok","f":"publish","value":1,"partition":0,"offset":0}`,
		`{"process":"producer","type":"invoke","f":"publish","value":2}`,
		`{"process":"producer","type":"ok","f":"publish","value":2,"partition":0,"offset":1}`,
		`{"process":"final-read","type":"ok","f":"read","value":1,"partition":0,"offset":0}`,
		`{"process":"final-read","type":"ok","f":"read","value":2,"partition":0,"offset":1}`,
	}
	sameLines(t, untimed(t, out.String()), want)

	calls := []string{`prepare driftlab-test 1 ""`, "read back"}
	if got := fc.called(); !slices.Equal(got, calls) {
		t.Errorf("asked the cluster %q, want %q", got, calls)
	}
}

// A driver that breaks ends the run. After its ready, the history says how
// it broke and has no final read, and the run first waits for the fault to
// end, so no broker is left faulted. Before its ready, no run began, so
// there is no history at all.
func TestRunBreaks(t *testing.T) {
	for _, tc := range []struct {
		fake  string
		fault fault
		want  string   // what the run's error says
		broke string   // the history's broke line, or "" for no history
		calls []string // what the run asked of the cluster
	}{
		{
			fake:  "wrong-value",
			fault: fault{kind: "pause", at: 1, hold: testHold, live: true},
			want:  "the driver broke: answered publish 1 with taken 2",
			broke: `{"process":"driver","type":"info","f":"broke","error":"answered publish 1 with taken 2"}`,
			calls: []string{`prepare driftlab-test 1 "pause"`, "start pause", "end pause"},
		},
		{
			fake:  "reports-first",
			want:  "the driver broke: wrote error before ready",
			calls: []string{`prepare driftlab-test 1 ""`},
		},
	} {
		t.Run(tc.fake, func(t *testing.T) {
			p := testPlan(t, tc.fake)
			p.fault = tc.fault
			fc := &fakeCluster{}
			var out, logs bytes.Buffer
			err := run(p, fc, &out, &logs)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got error %v, want one containing %q", err, tc.want)
			}
			if got := fc.called(); !slices.Equal(got, tc.calls) {
				t.Errorf("asked the cluster %q, want %q", got, tc.calls)
			}
			lines := untimed(t, out.String())
			if tc.broke == "" {
				if len(lines) > 0 {
					t.Errorf("wrote a history, but no run began:\n%s", out.String())
				}
				return
			}
			if !slices.Contains(lines, tc.broke) {
				t.Errorf("the history has no line %s:\n%s", tc.broke, out.String())
			}
			if strings.Contains(out.String(), `"final-read"`) {
				t.Errorf("read the topic back after the driver broke:\n%s", out.String())
			}
		})
	}
}

// The wait for the reader ends as soon as the reader has delivered up to the
// ends and they haven't moved, and otherwise when the grace runs out.
func TestWaitForReader(t *testing.T) {
	stored := map[int32][]uint64{0: {1, 3}, 1: {2}}
	pr := newProgress()
	pr.saw(event{Event: "deliver", Value: 1, Partition: 0, Offset: 0})
	pr.saw(event{Event: "deliver", Value: 3, Partition: 0, Offset: 1})

	// Partition 1's value hasn't been delivered, so the wait lasts the grace.
	grace := 200 * time.Millisecond
	began := time.Now()
	waitForReader(context.Background(), &fakeCluster{stored: stored}, pr, "t", 2, grace)
	if took := time.Since(began); took < grace {
		t.Errorf("stopped waiting after %v with the reader behind, before the grace of %v ran out", took, grace)
	}

	// Once it has been, the wait asks twice: once to find the ends reached,
	// and once more to see they haven't moved.
	pr.saw(event{Event: "deliver", Value: 2, Partition: 1, Offset: 0})
	fc := &fakeCluster{stored: stored}
	waitForReader(context.Background(), fc, pr, "t", 2, 5*time.Second)
	if got, want := fc.called(), []string{"ends", "ends"}; !slices.Equal(got, want) {
		t.Errorf("asked the cluster %q, want %q", got, want)
	}
}
