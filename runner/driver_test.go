package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeEnv names the environment variable that turns this test program into
// a fake driver. The tests start the program again with it set, and TestMain
// then runs the fake it names instead of the tests.
const fakeEnv = "DRIFTLAB_FAKE_DRIVER"

func TestMain(m *testing.M) {
	if name := os.Getenv(fakeEnv); name != "" {
		os.Exit(fake(name))
	}
	os.Exit(m.Run())
}

// fake acts as a driver. It answers each command the way a driver should,
// except where its name says it breaks a rule, and returns its exit status.
// Once its reader has started, it delivers each value it publishes. sync
// keeps every rule, but reports on each value before it answers the
// publish, as a driver of a synchronous client would: one whose send call
// returns only once the value is acknowledged. times-out keeps every rule
// too, but reports each value as an info with code 7, REQUEST_TIMED_OUT,
// before it answers, and delivers nothing.
func fake(name string) int {
	fmt.Fprintln(os.Stderr, "fake", name+": started")
	if name == "reports-first" {
		say(`{"event":"error","process":"producer","error":"too soon"}`)
	}
	offsets := map[int32]int64{}
	reading := false // the reader has started
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		var c struct {
			Cmd       string `json:"cmd"`
			Value     uint64 `json:"value"`
			Partition int32  `json:"partition"`
		}
		if err := json.Unmarshal(in.Bytes(), &c); err != nil {
			fmt.Fprintln(os.Stderr, "fake", name+":", err)
			return 2
		}
		switch c.Cmd {
		case "setup":
			say(`{"event":"ready","driver":"fake","client":"fake@1","settings":{"acks":"all"}}`)
			if name == "quits-early" {
				return 3
			}
		case "subscribe":
			switch name {
			case "wrong-answer":
				say(`{"event":"writer-closed"}`)
			case "ready-twice":
				say(`{"event":"ready","driver":"fake","client":"fake@1","settings":{}}`)
			case "closes-stdout":
				os.Stdout.Close()
				time.Sleep(time.Hour)
			default:
				say(`{"event":"subscribed"}`)
				reading = true
			}
		case "publish":
			switch name {
			case "hangs":
				// takes the value and never answers
			case "writes-time":
				say(`{"event":"taken","value":%d,"time_ns":5}`, c.Value)
			case "wrong-value":
				say(`{"event":"taken","value":%d}`, c.Value+1)
			case "times-out":
				say(`{"event":"info","value":%d,"error":"request timed out","code":7}`, c.Value)
				say(`{"event":"taken","value":%d}`, c.Value)
			default:
				o := offsets[c.Partition]
				offsets[c.Partition]++
				if name != "sync" {
					say(`{"event":"taken","value":%d}`, c.Value)
				}
				say(`{"event":"ok","value":%d,"partition":%d,"offset":%d}`, c.Value, c.Partition, o)
				if reading {
					say(`{"event":"deliver","value":%d,"partition":%d,"offset":%d,"poll":%d}`, c.Value, c.Partition, o, c.Value)
				}
				if name == "sync" {
					say(`{"event":"taken","value":%d}`, c.Value)
				}
			}
		case "close-writer":
			say(`{"event":"writer-closed"}`)
		case "stop-reader":
			say(`{"event":"reader-stopped"}`)
			if name == "answers-twice" {
				say(`{"event":"reader-stopped"}`)
			}
		}
	}
	// The runner closed stdin: the run is over.
	switch name {
	case "lingers":
		time.Sleep(time.Hour)
	case "fails-at-end":
		return 1
	}
	fmt.Fprintln(os.Stderr, "fake", name+": done")
	return 0
}

// say writes one event on stdout. os.Stdout has no buffer in Go, so each
// line goes out at once, as rule 1 for a driver asks.
func say(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

// startFake starts this test program again as the fake driver called name. It
// returns the driver, and the events and the log the driver writes, which
// are complete once the driver has exited.
func startFake(t *testing.T, name string, deadline time.Duration) (*driver, *[]event, *bytes.Buffer) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(fakeEnv, name)
	// Under go test -race, the fake is built with the race detector, which
	// pauses for a second before a program exits with status 0, to give a
	// race still running a last chance to show. The fake skips that pause,
	// which alone would miss the 1-second deadline TestDriverBreaks gives it.
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	events := []event{}
	logs := &bytes.Buffer{}
	clock := func() time.Duration { return 1500 * time.Millisecond }
	d, err := startDriver([]string{exe}, func(e event) { events = append(events, e) }, logs, clock, deadline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.kill) // whatever happens in the test, no fake outlives it
	return d, &events, logs
}

// converse takes a driver through a whole run's commands, as a run would,
// and returns the first error.
func converse(d *driver) error {
	for _, c := range []command{
		newSetup([]string{"127.0.0.1:29092"}, "t", 2, "defaults", nil),
		subscribeCmd,
		newPublish(1, 0),
		newPublish(2, 1),
		newPublish(3, 0),
		closeWriterCmd,
		stopReaderCmd,
	} {
		if _, err := d.ask(c); err != nil {
			return err
		}
	}
	return d.finish()
}

// brief names an event, with its value and position when it has them.
func brief(e event) string {
	switch e.Event {
	case "taken":
		return fmt.Sprintf("taken %d", e.Value)
	case "ok", "deliver":
		return fmt.Sprintf("%s %d at %d/%d", e.Event, e.Value, e.Partition, e.Offset)
	}
	return e.Event
}

// A driver that keeps to the protocol gets through a whole run, and the
// runner gets every event it wrote, in order, and its log, with the clock in
// front of each line.
func TestDriverBehaves(t *testing.T) {
	d, events, logs := startFake(t, "behaves", 5*time.Second)
	if err := converse(d); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range *events {
		got = append(got, brief(e))
	}
	want := []string{
		"ready", "subscribed",
		"taken 1", "ok 1 at 0/0", "deliver 1 at 0/0",
		"taken 2", "ok 2 at 1/0", "deliver 2 at 1/0",
		"taken 3", "ok 3 at 0/1", "deliver 3 at 0/1",
		"writer-closed", "reader-stopped",
	}
	if !slices.Equal(got, want) {
		t.Errorf("events:\ngot  %q\nwant %q", got, want)
	}
	if out := logs.String(); out != "1.500 fake behaves: started\n1.500 fake behaves: done\n" {
		t.Errorf("log: got %q", out)
	}
}

// A driver that breaks the protocol, in each way a driver can, is caught,
// and the error says how it broke.
func TestDriverBreaks(t *testing.T) {
	for _, tc := range []struct{ fake, want string }{
		{"quits-early", "exited before its stdin closed (exit status 3)"},
		{"writes-time", `taken doesn't take "time_ns"`},
		{"hangs", "didn't answer publish within 1s"},
		{"wrong-value", "answered publish 1 with taken 2"},
		{"wrong-answer", "answered subscribe with writer-closed"},
		{"reports-first", "wrote error before ready"},
		{"ready-twice", "wrote ready twice"},
		{"answers-twice", "wrote reader-stopped, an answer to no command"},
		{"closes-stdout", "closed its stdout but didn't exit"},
		{"lingers", "didn't exit within 1s of its stdin closing"},
		{"fails-at-end", "exited with exit status 1"},
	} {
		t.Run(tc.fake, func(t *testing.T) {
			d, _, _ := startFake(t, tc.fake, time.Second)
			err := converse(d)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got error %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// logWriter puts the clock in front of every line, even when a line comes in
// two pieces, or one piece holds several lines.
func TestLogWriter(t *testing.T) {
	var out bytes.Buffer
	w := &logWriter{out: &out, clock: func() time.Duration { return 1500 * time.Millisecond }}
	w.Write([]byte("a\nb"))
	w.Write([]byte("c\n\nd\n"))
	if got, want := out.String(), "1.500 a\n1.500 bc\n1.500 \n1.500 d\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
