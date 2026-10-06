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

// scenario describes one experiment. A zero stall means no fault is
// injected.
type scenario struct {
	consumer bool
	stall    time.Duration
}

var scenarios = map[string]scenario{
	"publish-only": {},
	"control":      {consumer: true},
	"slow-short":   {consumer: true, stall: 2 * time.Second},
	"slow-long":    {consumer: true, stall: 8 * time.Second},
}

type config struct {
	url      string
	scenario string
	api      string
	n        uint64
	pending  int
	grace    time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", nats.DefaultURL, "NATS server to run against")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only, control, slow-short or slow-long")
	flag.StringVar(&c.api, "api", "legacy", "consumer API: legacy (push) or jetstream (pull)")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish")
	flag.IntVar(&c.pending, "pending", 20, "the consumer's tray size during a stall, in messages")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the consumer gets to catch up before the final read")
	flag.Parse()
	if err := run(c, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "natsdriver:", err)
		os.Exit(1)
	}
}

func run(c config, out io.Writer) error {
	sc, ok := scenarios[c.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", c.scenario)
	}
	if c.api != "legacy" && c.api != "jetstream" {
		return fmt.Errorf("unknown api %q", c.api)
	}
	if c.n < 1 {
		return errors.New("-n must be at least 1")
	}
	rec := newRecorder(out)

	// nats.go reports some problems in the background instead of returning
	// them. A slow consumer error is also the proof that the overflow fault
	// happened: the client's pending tray filled up and it dropped messages.
	var overflowed sync.Once
	onError := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: err.Error()})
		if errors.Is(err, nats.ErrSlowConsumer) {
			overflowed.Do(func() {
				rec.op(op{Process: "nemesis", Type: "ok", F: "overflow"})
			})
		}
	}
	nc, err := nats.Connect(c.url, nats.ErrorHandler(onError))
	if err != nil {
		return fmt.Errorf("connecting to %s: %w (is the server up? docker compose -f deploy/compose.yaml up -d)", c.url, err)
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

	rec.writeMeta(newMeta(nc.ConnectedServerVersion(), c, sc))
	if sc.consumer {
		if err := consume(ctx, nc, js, rec, c, sc); err != nil {
			return err
		}
	} else {
		for v := uint64(1); v <= c.n; v++ {
			publish(ctx, js, rec, v)
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

// consume runs an ordered consumer through the scenario. Only the first
// message is published before it subscribes. The rest follow once that
// first message has arrived, which is where a stall scenario's callback
// blocks, so the flood always lands after the trap is armed. Shrinking the
// tray first matters: it only turns away new arrivals, so a backlog that
// arrived earlier would never overflow it. One more message follows once
// the consumer has caught up, and it stops when it has seen everything or
// the grace window runs out.
func consume(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, rec *recorder, c config, sc scenario) error {
	// reached is the highest stream sequence delivered so far. Only atomic
	// operations touch it: waitFor reads it from this goroutine, and onMsg
	// can run on more than one goroutine, because the jetstream consumer
	// starts a new subscription every time it resets.
	var reached atomic.Uint64
	gate := make(chan struct{})
	var stalled sync.Once
	onErr := func(err error) {
		rec.op(op{Process: "consumer", Type: "info", F: "client-error", Error: err.Error()})
	}
	onMsg := func(data []byte, seq uint64) {
		v, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			onErr(err)
			return
		}
		rec.op(op{Process: "consumer", Type: "ok", F: "deliver", Value: v, StreamSeq: seq})
		raise(&reached, seq)
		if sc.stall > 0 {
			stalled.Do(func() { <-gate })
		}
	}

	last := publish(ctx, js, rec, 1)
	rec.op(op{Process: "consumer", Type: "invoke", F: "subscribe"})
	var rd reader
	var err error
	if c.api == "jetstream" {
		rd, err = subscribeJetStream(ctx, js, c, sc, onMsg, onErr)
	} else {
		rd, err = subscribeLegacy(nc, c, onMsg, onErr)
	}
	if err != nil {
		rec.op(op{Process: "consumer", Type: "info", F: "subscribe", Error: err.Error()})
		return err
	}
	rec.op(op{Process: "consumer", Type: "ok", F: "subscribe"})
	waitFor(&reached, last, c.grace)

	// The 2107 trigger: a blocked callback while the flood arrives behind it,
	// and, where the API has one, a tiny pending tray for the flood to overflow.
	if sc.stall > 0 {
		if rd.shrinkTray != nil {
			rec.op(op{Process: "nemesis", Type: "invoke", F: "overflow"})
			if err := rd.shrinkTray(); err != nil {
				return err
			}
		}
		rec.op(op{Process: "nemesis", Type: "invoke", F: "stall"})
	}
	for v := uint64(2); v <= c.n; v++ {
		last = max(last, publish(ctx, js, rec, v))
	}
	if sc.stall > 0 {
		time.Sleep(sc.stall)
		close(gate)
		rec.op(op{Process: "nemesis", Type: "ok", F: "stall"})
	}

	waitFor(&reached, last, c.grace)
	last = max(last, publish(ctx, js, rec, c.n+1))
	waitFor(&reached, last, c.grace)
	return rd.stop()
}

// reader is a running consumer: how to stop it, and how to shrink its
// pending tray. shrinkTray is nil when the API has no tray that can
// overflow.
type reader struct {
	stop       func() error
	shrinkTray func() error
}

// subscribeLegacy uses the legacy push ordered consumer, the API that KV
// and Object Store watchers use. The server pushes messages at it, and
// whatever doesn't fit its pending tray is dropped.
func subscribeLegacy(nc *nats.Conn, c config, onMsg func([]byte, uint64), onErr func(error)) (reader, error) {
	legacy, err := nc.JetStream()
	if err != nil {
		return reader{}, err
	}
	sub, err := legacy.Subscribe(subject, func(m *nats.Msg) {
		md, err := m.Metadata()
		if err != nil {
			onErr(err)
			return
		}
		onMsg(m.Data, md.Sequence.Stream)
	}, nats.OrderedConsumer())
	if err != nil {
		return reader{}, err
	}
	return reader{
		stop:       sub.Unsubscribe,
		shrinkTray: func() error { return sub.SetPendingLimits(c.pending, 8*1024*1024) },
	}, nil
}

// subscribeJetStream uses the ordered consumer from the newer jetstream
// package. It pulls messages in batches of at most PullMaxMessages, asking
// for more only when there is room, so its tray can never overflow and
// there is nothing to shrink.
func subscribeJetStream(ctx context.Context, js jetstream.JetStream, c config, sc scenario, onMsg func([]byte, uint64), onErr func(error)) (reader, error) {
	cons, err := js.OrderedConsumer(ctx, streamName, jetstream.OrderedConsumerConfig{})
	if err != nil {
		return reader{}, err
	}
	opts := []jetstream.PullConsumeOpt{
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) { onErr(err) }),
	}
	if sc.stall > 0 {
		opts = append(opts, jetstream.PullMaxMessages(c.pending))
	}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		md, err := m.Metadata()
		if err != nil {
			onErr(err)
			return
		}
		onMsg(m.Data(), md.Sequence.Stream)
	}, opts...)
	if err != nil {
		return reader{}, err
	}
	return reader{stop: func() error { cc.Stop(); return nil }}, nil
}

// waitFor polls until the consumer has reached target or grace runs out.
func waitFor(reached *atomic.Uint64, target uint64, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for reached.Load() < target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// raise sets n to v unless n already holds a larger value. It is safe on
// any number of goroutines: if another one changes n between the Load and
// the CompareAndSwap, the swap fails and the loop reads n again.
func raise(n *atomic.Uint64, v uint64) {
	for {
		cur := n.Load()
		if v <= cur || n.CompareAndSwap(cur, v) {
			return
		}
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
	Type     string   `json:"type"`
	Driver   string   `json:"driver"`
	Client   string   `json:"client"`
	API      string   `json:"api,omitempty"`
	Server   string   `json:"server"`
	Scenario string   `json:"scenario"`
	Faults   []string `json:"faults,omitempty"`
	Params   params   `json:"params"`
	Driftlab string   `json:"driftlab"`
}

type params struct {
	N           uint64 `json:"n"`
	PendingMsgs int    `json:"pending_msgs,omitempty"`
	Stall       string `json:"stall,omitempty"`
}

// newMeta describes the run. A stall scenario declares the stall, and for
// the legacy API also the overflow it is meant to cause. If a declared
// fault never happens, the checker calls the run invalid instead of clean.
func newMeta(server string, c config, sc scenario) metaLine {
	client, rev := provenance()
	m := metaLine{
		Type:     "meta",
		Driver:   "natsgo",
		Client:   client,
		Server:   "nats-server " + server,
		Scenario: c.scenario,
		Params:   params{N: c.n},
		Driftlab: rev,
	}
	if sc.consumer {
		m.API = c.api
	}
	if sc.stall > 0 {
		m.Faults = []string{"stall"}
		if c.api == "legacy" {
			m.Faults = append(m.Faults, "overflow")
		}
		m.Params.PendingMsgs = c.pending
		m.Params.Stall = sc.stall.String()
	}
	return m
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

func (r *recorder) writeMeta(m metaLine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = r.enc.Encode(m)
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
