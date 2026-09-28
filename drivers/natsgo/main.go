// Command natsdriver runs one driftlab scenario against a NATS server and
// writes its history to stdout, in the format DESIGN.md specifies.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName = "DRIFTLAB"
	subject    = "driftlab"
)

type config struct {
	url      string
	scenario string
	n        uint64
	grace    time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", nats.DefaultURL, "NATS server to run against")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only or control")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish before the consumer starts")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the consumer gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "natsdriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	if c.scenario != "publish-only" && c.scenario != "control" {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	nc, err := nats.Connect(c.url)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx := context.Background()
	stream, err := freshStream(ctx, js)
	if err != nil {
		return err
	}

	rec := newRecorder(out)
	api := ""
	if c.scenario != "publish-only" {
		api = "legacy"
	}
	rec.writeMeta(nc.ConnectedServerVersion(), c.scenario, api)
	var last uint64
	for v := uint64(1); v <= c.n; v++ {
		last = max(last, publish(ctx, js, rec, v))
	}
	if c.scenario == "control" {
		if err := consume(ctx, nc, js, rec, c, last); err != nil {
			return err
		}
	}
	return finalRead(ctx, stream, rec)
}

// freshStream replaces any stream left over from an earlier run, so every
// history starts at sequence 1. Deletes and purges are denied, because the
// checker would report a deleted message as a lost write (DESIGN.md
// principle 5).
func freshStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	if err := js.DeleteStream(ctx, streamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, err
	}
	return js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subject},
		Storage:    jetstream.FileStorage,
		Replicas:   1,
		DenyDelete: true,
		DenyPurge:  true,
	})
}

// publish records one publish and returns its stream sequence, or 0 when
// the outcome is unknown.
func publish(ctx context.Context, js jetstream.JetStream, rec *recorder, v uint64) uint64 {
	rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
	ack, err := js.Publish(ctx, subject, []byte(strconv.FormatUint(v, 10)))
	if err != nil {
		rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
		return 0
	}
	rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, StreamSeq: ack.Sequence})
	return ack.Sequence
}

// consume runs the legacy push ordered consumer, the API that KV and Object
// Store watchers use. Once it has caught up on the backlog, one more
// message is published behind it. It stops as soon as it has seen
// everything, or when the grace window runs out.
func consume(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, rec *recorder, c config, last uint64) error {
	legacy, err := nc.JetStream()
	if err != nil {
		return err
	}
	var reached atomic.Uint64
	var highest uint64 // only the callback touches this; nats.go calls it on one goroutine
	handler := func(m *nats.Msg) {
		md, err := m.Metadata()
		if err != nil {
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: err.Error()})
			return
		}
		v, err := strconv.ParseUint(string(m.Data), 10, 64)
		if err != nil {
			rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: err.Error()})
			return
		}
		rec.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: v, StreamSeq: md.Sequence.Stream})
		if md.Sequence.Stream > highest {
			highest = md.Sequence.Stream
			reached.Store(highest)
		}
	}

	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	sub, err := legacy.Subscribe(subject, handler, nats.OrderedConsumer())
	if err != nil {
		rec.op(op{Process: "consumer", Type: "info", F: "subscribe", Error: err.Error()})
		return err
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})

	waitFor(&reached, last, c.grace)
	last = max(last, publish(ctx, js, rec, c.n+1))
	waitFor(&reached, last, c.grace)
	return sub.Unsubscribe()
}

// waitFor polls until the consumer has reached target or grace runs out.
func waitFor(reached *atomic.Uint64, target uint64, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for reached.Load() < target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// finalRead reads every stream sequence back by number, without a
// consumer: the ground truth must not depend on the machinery under test.
func finalRead(ctx context.Context, stream jetstream.Stream, rec *recorder) error {
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	for seq := uint64(1); seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		v, err := strconv.ParseUint(string(msg.Data), 10, 64)
		if err != nil {
			return fmt.Errorf("stream_seq %d holds %q, not a driftlab value", seq, msg.Data)
		}
		rec.op(op{Process: "final-read", Type: "ok", F: "read", Value: v, StreamSeq: seq})
	}
	return rec.Err()
}

type op struct {
	Index     int    `json:"index"`
	TimeNS    int64  `json:"time_ns"`
	Process   string `json:"process"`
	Type      string `json:"type"`
	F         string `json:"f"`
	Value     uint64 `json:"value,omitempty"`
	StreamSeq uint64 `json:"stream_seq,omitempty"`
	Error     string `json:"error,omitempty"`
}

type metaLine struct {
	Type     string `json:"type"`
	Driver   string `json:"driver"`
	Client   string `json:"client"`
	API      string `json:"api,omitempty"`
	Server   string `json:"server"`
	Scenario string `json:"scenario"`
	Driftlab string `json:"driftlab"`
}

// recorder writes the history. Its mutex guards every other field, because
// the consumer callback records from its own goroutine.
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

func (r *recorder) writeMeta(server, scenario, api string) {
	client, rev := provenance()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = r.enc.Encode(metaLine{
		Type:     "meta",
		Driver:   "natsgo",
		Client:   client,
		API:      api,
		Server:   "nats-server " + server,
		Scenario: scenario,
		Driftlab: rev,
	})
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

// provenance returns the nats.go version linked into this binary and the
// driftlab commit it was built from, so every history says what produced it.
func provenance() (client, rev string) {
	client, rev = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return client, rev
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/nats-io/nats.go" {
			client = dep.Path + "@" + dep.Version
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
	return client, rev
}
