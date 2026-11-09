package main

import (
	"flag"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"
)

// parse reads a command line, given as one string, as main does, with a
// flag set of its own. It returns what the line says, and the first error.
func parse(line string) (cli, error) {
	fs := flag.NewFlagSet("driftlab-run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c := defineFlags(fs)
	err := fs.Parse(strings.Fields(line))
	c.driver = fs.Args()
	return *c, err
}

// testTime is when the tests' plans are made. Its milliseconds name the
// topic.
var testTime = time.UnixMilli(1762600000000)

// mustPlan makes the plan for a command line that should give one.
func mustPlan(t *testing.T, line string) plan {
	t.Helper()
	c, err := parse(line)
	if err != nil {
		t.Fatal(err)
	}
	p, err := newPlan(c, testTime, "github.com/twmb/franz-go@v1.20.7", "test")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// With no flags, the defaults fill in what control leaves open, and the
// driver's command is everything after --, its own flags included.
func TestNewPlanDefaults(t *testing.T) {
	p := mustPlan(t, "-- bin/driver -v")
	want := plan{
		scenario:   "control",
		topic:      "driftlab-control-1762600000000",
		n:          100,
		partitions: 3,
		interval:   10 * time.Millisecond,
		reader:     true,
		settings:   "defaults",
		options:    map[string]string{},
		grace:      10 * time.Second,
		brokers:    []string{"127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092"},
		server:     "apache/kafka:4.3.1",
		witness:    "github.com/twmb/franz-go@v1.20.7",
		driftlab:   "test",
		driver:     []string{"bin/driver", "-v"},
		deadline:   driverDeadline,
	}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("got  %+v\nwant %+v", p, want)
	}
}

// A scenario's own numbers beat -n and -partitions, which only fill in what
// it leaves open. -interval, -hold and -settings change the run, and
// -option replaces one of the scenario's options and adds another, without
// changing the scenario itself.
func TestNewPlanScenario(t *testing.T) {
	p := mustPlan(t, "-scenario followers-cut -n 7 -partitions 5 -interval 2ms -hold 15s -settings idempotent -option produce_timeout=5s -option max_batch_messages=1 -- bin/driver")
	wantOptions := map[string]string{"produce_timeout": "5s", "max_batch_messages": "1"}
	if !maps.Equal(p.options, wantOptions) {
		t.Errorf("options %v, want %v", p.options, wantOptions)
	}
	if own := scenarios["followers-cut"].options; !maps.Equal(own, map[string]string{"produce_timeout": "2s"}) {
		t.Errorf("the scenario's own options became %v", own)
	}
	p.options = nil // checked above
	want := plan{
		scenario:   "followers-cut",
		topic:      "driftlab-followers-cut-1762600000000",
		n:          3200,
		partitions: 1,
		interval:   2 * time.Millisecond,
		settings:   "idempotent",
		fault:      fault{kind: "cut", at: 201, hold: 15 * time.Second, timeout: true},
		grace:      10 * time.Second,
		brokers:    []string{"127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092"},
		server:     "apache/kafka:4.3.1",
		witness:    "github.com/twmb/franz-go@v1.20.7",
		driftlab:   "test",
		driver:     []string{"bin/driver"},
		deadline:   driverDeadline,
	}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("got  %+v\nwant %+v", p, want)
	}
}

// The runner refuses a command line it can't carry out, and says why.
func TestRefuses(t *testing.T) {
	for _, tc := range []struct{ line, want string }{
		{"-scenario nope -- bin/driver", `unknown scenario "nope"`},
		{"-scenario control", "no driver command"},
		{"-interval -1ms -- bin/driver", "can't be negative"},
		{"-n 0 -- bin/driver", "-n must be at least 1"},
		{"-partitions 0 -- bin/driver", "-partitions must be at least 1"},
		{"-option produce_timeout -- bin/driver", "want name=value"},
		{"-option =5s -- bin/driver", "want name=value"},
		{"-option produce_timeout=5s -option produce_timeout=6s -- bin/driver", "produce_timeout is given twice"},
	} {
		c, err := parse(tc.line)
		if err == nil {
			_, err = newPlan(c, testTime, "w", "d")
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got error %v, want one containing %q", tc.line, err, tc.want)
		}
	}
}

// A fault must start before a value the run sends.
func TestPlanCheckFault(t *testing.T) {
	p := plan{n: 3, partitions: 1, fault: fault{kind: "kill", at: 4}}
	if err := p.check(); err == nil {
		t.Error("passed a fault that starts before value 4 of 3")
	}
	p.fault.at = 3
	if err := p.check(); err != nil {
		t.Errorf("refused a fault that starts before value 3 of 3: %v", err)
	}
}

// Every scenario makes a plan the runner can carry out. The five faults of
// README.md's table start live, and the faults outside it on the loop
// (DESIGN.md, Every table fault starts live).
func TestScenarios(t *testing.T) {
	live := map[string]bool{"leader-kill-live": true, "leader-kill-steady": true, "followers-pause": true, "follower-pause": true, "follower-cut": true}
	for name := range scenarios {
		p := mustPlan(t, "-scenario "+name+" -- bin/driver")
		if p.fault.kind != "" && p.fault.live != live[name] {
			t.Errorf("%s: live is %v, want %v", name, p.fault.live, live[name])
		}
	}
}
