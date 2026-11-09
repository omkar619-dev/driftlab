// Command driftlab-run runs one of driftlab's Kafka scenarios through a
// driver: a program, in any language, that writes and reads through the
// client under test (DESIGN.md, Driver split).
//
//	driftlab-run [flags] -- <driver command>
//
// The runner does the rest. It creates the topic, starts the driver and
// tells it what to send, puts the scenario's fault in place, waits for the
// driver's reader, and reads the topic back with franz-go, the witness. It
// writes the history to stdout, and the driver's log, then its own report,
// to stderr.
package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"time"
)

// scenario is one experiment. With reader set, the driver reads the topic
// while it is written. A non-zero partitions or n replaces the -partitions
// or -n flag. A non-zero interval is the pause after each value unless
// -interval sets one, and the pause is 10 milliseconds otherwise. options
// are the scenario's own changes to single producer settings, which -option
// can change again. The fault, if the scenario has one, is as run.go
// describes it, and -hold can change how long it lasts.
type scenario struct {
	reader     bool
	partitions int
	n          uint64
	interval   time.Duration
	options    map[string]string
	fault      fault
}

// scenarios are the Kafka drivers' scenarios, with their numbers.
//
// publish-only writes the topic and reads it back with the witness alone;
// control adds the driver's reader.
//
// timeout-control, followers-cut and followers-cut-single are sarama's
// calibration (DESIGN.md, sarama results). They use one partition, so every
// value meets the cut, and a 2-second produce timeout. While the followers
// are cut off, the leader can't get a batch copied, so it answers
// REQUEST_TIMED_OUT after 2 seconds, and sarama's 3 retries run out about 8
// seconds in, while the cut holds. The cut holds 20 seconds, under the 30
// after which the leader would drop the followers. timeout-control is the
// same run without the cut, and followers-cut-single sends one value per
// request. Each cut also declares the timeout (run.go, timeoutWatch).
//
// leader-kill kills the broker that leads the partition just before value
// 501 of 3,000, and starts it again 20 seconds later. leader-kill-live does
// the same while values go out a millisecond apart, 20,000 of them, and
// leader-kill-steady just before value 5,001, once the followers have long
// been copying.
//
// followers-pause freezes both brokers that hold the partition's other
// copies for 25 seconds, and follower-pause one of them. follower-cut cuts
// one of them off from the leader for 50 seconds, so the leader drops it
// from the in-sync set, 30 to 45 seconds in.
//
// The five faults of README.md's table start live. sarama's calibration and
// leader-kill, outside the table, start on the loop, as drivers/sarama
// started them (DESIGN.md, Every table fault starts live).
var scenarios = map[string]scenario{
	"publish-only":         {},
	"control":              {reader: true},
	"timeout-control":      {partitions: 1, n: 3200, options: map[string]string{"produce_timeout": "2s"}},
	"followers-cut":        {partitions: 1, n: 3200, options: map[string]string{"produce_timeout": "2s"}, fault: fault{kind: "cut", at: 201, hold: 20 * time.Second, timeout: true}},
	"followers-cut-single": {partitions: 1, n: 3200, options: map[string]string{"produce_timeout": "2s", "max_batch_messages": "1"}, fault: fault{kind: "cut", at: 201, hold: 20 * time.Second, timeout: true}},
	"leader-kill":          {reader: true, partitions: 1, n: 3000, fault: fault{kind: "kill", at: 501, hold: 20 * time.Second}},
	"leader-kill-live":     {reader: true, partitions: 1, n: 20000, interval: time.Millisecond, fault: fault{kind: "kill", at: 501, hold: 20 * time.Second, live: true}},
	"leader-kill-steady":   {reader: true, partitions: 1, n: 20000, interval: time.Millisecond, fault: fault{kind: "kill", at: 5001, hold: 20 * time.Second, live: true}},
	"followers-pause":      {reader: true, partitions: 1, n: 4000, fault: fault{kind: "pause", at: 501, hold: 25 * time.Second, live: true}},
	"follower-pause":       {reader: true, partitions: 1, n: 4000, fault: fault{kind: "pause", followers: 1, at: 501, hold: 25 * time.Second, live: true}},
	"follower-cut":         {reader: true, partitions: 1, n: 6500, fault: fault{kind: "cut", followers: 1, at: 501, hold: 50 * time.Second, live: true}},
}

// cli is what the command line says: the flags, then the driver's command.
type cli struct {
	brokers    string
	server     string
	scenario   string
	n          uint64
	partitions int
	interval   time.Duration
	hold       time.Duration
	grace      time.Duration
	settings   string
	options    map[string]string
	nftImage   string
	cutPort    int
	driver     []string
}

func main() {
	c := defineFlags(flag.CommandLine)
	flag.Usage = usage
	flag.Parse()
	c.driver = flag.Args()
	if err := execute(*c); err != nil {
		fmt.Fprintln(os.Stderr, "driftlab-run:", err)
		os.Exit(1)
	}
}

// usage is what -h prints.
func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintln(out, "usage: driftlab-run [flags] -- <driver command>")
	fmt.Fprintln(out, "Runs one scenario through the driver, and writes its history to stdout.")
	flag.PrintDefaults()
}

// defineFlags defines the runner's flags on fs, and returns the cli they
// fill in as fs parses the command line. What follows the flags, the
// driver's command, is left to the caller.
func defineFlags(fs *flag.FlagSet) *cli {
	c := &cli{options: map[string]string{}}
	names := strings.Join(slices.Sorted(maps.Keys(scenarios)), ", ")
	fs.StringVar(&c.brokers, "brokers", "127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092", "Kafka brokers to start from, comma separated")
	fs.StringVar(&c.server, "server", "apache/kafka:4.3.1", "the broker image, recorded in the meta line (deploy/kafka.yaml pins it)")
	fs.StringVar(&c.scenario, "scenario", "control", "the scenario to run: "+names)
	fs.Uint64Var(&c.n, "n", 100, "number of values to publish, unless the scenario sets it")
	fs.IntVar(&c.partitions, "partitions", 3, "number of partitions in the run's topic, unless the scenario sets it")
	fs.DurationVar(&c.interval, "interval", 0, "the pause after each value is taken; 0 keeps the scenario's own, or 10ms")
	fs.DurationVar(&c.hold, "hold", 0, "how long the fault lasts: how long the cut or the freeze holds, or how long a killed broker stays down; 0 keeps the scenario's own")
	fs.DurationVar(&c.grace, "grace", 10*time.Second, "how long the driver's reader gets to catch up before the final read")
	fs.StringVar(&c.settings, "settings", "defaults", "the named set of producer settings the driver writes with: defaults, the client as it ships, or a name the driver knows, such as sarama's idempotent")
	fs.Func("option", "a producer setting for this run, as name=value, such as produce_timeout=5s; give one for each setting, and it replaces the scenario's own value", func(s string) error {
		name, value, ok := strings.Cut(s, "=")
		if !ok || name == "" {
			return errors.New("want name=value")
		}
		if _, dup := c.options[name]; dup {
			return fmt.Errorf("%s is given twice", name)
		}
		c.options[name] = value
		return nil
	})
	fs.StringVar(&c.nftImage, "nft-image", "driftlab-nft", "the image the cut runs nft from, inside a broker's network (docker build -t driftlab-nft deploy/nft)")
	fs.IntVar(&c.cutPort, "cut-port", 19092, "the port the brokers fetch from each other on, which the cut drops (deploy/kafka.yaml's INTERNAL listener)")
	return c
}

// execute carries out the run the command line asks for, against the real
// cluster. It is apart from main so that the witness's client is closed
// before the runner exits: os.Exit skips deferred calls.
func execute(c cli) error {
	witness, driftlab := provenance()
	p, err := newPlan(c, time.Now(), witness, driftlab)
	if err != nil {
		return err
	}
	k, err := newKafka(p.brokers, config{nftImage: c.nftImage, cutPort: c.cutPort})
	if err != nil {
		return err
	}
	defer k.close()
	return run(p, k, os.Stdout, os.Stderr)
}

// newPlan works out the run from the command line, and refuses one that
// can't be carried out. The scenario sets the run, and the flags fill in
// what it leaves open and change what a run may change. The topic is the
// run's own, named for the scenario and the time, now. witness and driftlab
// say what built the runner.
func newPlan(c cli, now time.Time, witness, driftlab string) (plan, error) {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return plan{}, fmt.Errorf("unknown scenario %q", c.scenario)
	}
	if len(c.driver) == 0 {
		return plan{}, errors.New("no driver command: give it after --")
	}
	if c.interval < 0 || c.hold < 0 || c.grace < 0 {
		return plan{}, errors.New("-interval, -hold and -grace can't be negative")
	}
	// The options are the scenario's, then the flags', so a flag replaces
	// the scenario's value for the same setting. They go into a map of the
	// run's own: the scenario's map is shared, and must never change.
	options := map[string]string{}
	maps.Copy(options, sc.options)
	maps.Copy(options, c.options)
	// cmp.Or picks the first value that isn't zero: the scenario's number,
	// else the flag's; the flag's interval, else the scenario's, else 10ms.
	p := plan{
		scenario:   c.scenario,
		topic:      fmt.Sprintf("driftlab-%s-%d", c.scenario, now.UnixMilli()),
		n:          cmp.Or(sc.n, c.n),
		partitions: int32(cmp.Or(sc.partitions, c.partitions)),
		interval:   cmp.Or(c.interval, sc.interval, 10*time.Millisecond),
		reader:     sc.reader,
		settings:   c.settings,
		options:    options,
		fault:      sc.fault,
		grace:      c.grace,
		brokers:    strings.Split(c.brokers, ","),
		server:     c.server,
		witness:    witness,
		driftlab:   driftlab,
		driver:     c.driver,
		deadline:   driverDeadline,
	}
	// -hold changes only a fault, so a script can give it to every
	// scenario, those without a fault included.
	if p.fault.kind != "" && c.hold > 0 {
		p.fault.hold = c.hold
	}
	if err := p.check(); err != nil {
		return plan{}, err
	}
	return p, nil
}

// check refuses a plan that can't run: one with no values or no partitions,
// or a fault that would start before a value the run never sends.
func (p plan) check() error {
	switch {
	case p.n < 1:
		return errors.New("-n must be at least 1")
	case p.partitions < 1:
		return errors.New("-partitions must be at least 1")
	case p.fault.kind != "" && (p.fault.at < 1 || p.fault.at > p.n):
		return fmt.Errorf("the fault starts before value %d, but the run sends values 1 to %d", p.fault.at, p.n)
	}
	return nil
}

// provenance returns the franz-go linked into this binary, which the
// witness reads with, and the driftlab commit the runner was built from,
// marked -dirty if the build had changes not yet committed, so every
// history says what made it. What the binary doesn't record stays
// "unknown": a test binary, for one, records no commit.
func provenance() (witness, driftlab string) {
	witness, driftlab = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return witness, driftlab
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/twmb/franz-go" {
			witness = dep.Path + "@" + dep.Version
		}
	}
	dirty := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			driftlab = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if dirty {
		driftlab += "-dirty"
	}
	return witness, driftlab
}
