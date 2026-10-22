// Command natsdriver runs one driftlab scenario against a NATS server and
// writes its history to stdout, in the format DESIGN.md specifies.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName = "DRIFTLAB"
	subject    = "driftlab"

	// The consumer's line through toxiproxy (deploy/compose.yaml): it
	// listens on 4223 inside the toxiproxy container and leads to the nats
	// service.
	proxyName     = "helper-line"
	proxyListen   = "0.0.0.0:4223"
	proxyUpstream = "nats:4222"
)

// slowLatency is how late everything reaches the consumer in net-slow. The
// legacy ordered consumer checks for activity every 10 seconds and resets
// after a whole check interval with no message or heartbeat, so a line that
// stays quiet for longer than 20 seconds always trips it. The jetstream one
// resets only after 30 seconds of silence (see DESIGN.md), so it rides this
// delay out.
const slowLatency = 25 * time.Second

// scenario describes one experiment. A zero stall and empty net and server
// fields mean no fault is injected. With proxy set, the consumer reaches
// NATS through toxiproxy instead of directly. net names a fault on that
// line, "slow" or "cut", and server names one that hits nats-server itself,
// "pause" or "kill". Either starts halfway through the flood and lasts for
// hold.
type scenario struct {
	consumer bool
	startNew bool
	subDeath bool
	subs     int
	proxy    bool
	stall    time.Duration
	net      string
	server   string
	hold     time.Duration
}

var scenarios = map[string]scenario{
	"publish-only":  {},
	"control":       {consumer: true},
	"proxy-control": {consumer: true, proxy: true},
	"start-new":     {consumer: true, startNew: true},
	"sub-death":     {consumer: true, subDeath: true, subs: 300},
	"slow-short":    {consumer: true, stall: 2 * time.Second},
	"slow-long":     {consumer: true, stall: 8 * time.Second},
	"net-slow":      {consumer: true, proxy: true, net: "slow", hold: 30 * time.Second},
	"net-cut":       {consumer: true, proxy: true, net: "cut", hold: 8 * time.Second},
	"server-pause":  {consumer: true, server: "pause", hold: 35 * time.Second},
	"server-kill":   {consumer: true, server: "kill", hold: 5 * time.Second},
}

type config struct {
	url             string
	helperURL       string
	toxiproxy       string
	container       string
	scenario        string
	api             string
	n               uint64
	pending         int
	grace           time.Duration
	hold            time.Duration
	blockDisconnect time.Duration
	prefix          uint64
	subs            int
	reconnects      int
	reconnectGap    time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", nats.DefaultURL, "NATS server to run against")
	flag.StringVar(&c.helperURL, "helper-url", "nats://127.0.0.1:4223", "where the consumer connects in proxy scenarios: toxiproxy's helper-line")
	flag.StringVar(&c.toxiproxy, "toxiproxy", "http://127.0.0.1:8474", "toxiproxy's HTTP API, used by proxy scenarios")
	flag.StringVar(&c.container, "container", "driftlab-nats-1", "nats-server's docker container, which server scenarios pause or kill")
	flag.StringVar(&c.scenario, "scenario", "control", "publish-only, control, proxy-control, start-new, sub-death, slow-short, slow-long, net-slow, net-cut, server-pause or server-kill")
	flag.StringVar(&c.api, "api", "legacy", "consumer API: legacy (push) or jetstream (pull)")
	flag.Uint64Var(&c.n, "n", 100, "number of messages to publish")
	flag.IntVar(&c.pending, "pending", 20, "the consumer's tray size during a stall, in messages")
	flag.Uint64Var(&c.prefix, "prefix", 10, "in start-new, how many messages are published before the consumer subscribes past them")
	flag.IntVar(&c.subs, "subs", 0, "how many subscriptions to open; 0 means the scenario's own default, which is one")
	flag.IntVar(&c.reconnects, "reconnects", 10, "in sub-death, how many times to force a reconnect")
	flag.DurationVar(&c.reconnectGap, "reconnect-gap", 20*time.Millisecond, "in sub-death, the wait between reconnects; shorter than a consumer create is what lands a disconnect mid-create")
	flag.DurationVar(&c.grace, "grace", 10*time.Second, "how long the consumer gets to catch up before the final read")
	flag.DurationVar(&c.hold, "hold", 0, "how long a network or server fault lasts; 0 keeps the scenario's own")
	flag.DurationVar(&c.blockDisconnect, "block-disconnect", 0, "how long the consumer's disconnect handler blocks after it records a drop, as a slow application handler would; 0 returns at once")
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
	// -hold overrides how long a network or server fault lasts, to find
	// thresholds.
	if c.hold > 0 {
		sc.hold = c.hold
	}
	rec := newRecorder(out)

	// The producer's line. It creates the stream, publishes and does the
	// final read. It never goes through toxiproxy, so its acks keep coming
	// whatever happens to the consumer's line, but a server fault stops
	// them too.
	nc, err := nats.Connect(c.url)
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

	// The consumer's line is a connection of its own, so a fault can hit it
	// alone.
	var hc *nats.Conn
	if sc.consumer {
		if hc, err = connectHelper(c, sc, rec); err != nil {
			return err
		}
		defer hc.Close()
	}

	rec.writeMeta(newMeta(nc.ConnectedServerVersion(), c, sc))
	if sc.consumer {
		if err := consume(ctx, hc, js, rec, c, sc); err != nil {
			return err
		}
	} else {
		for v := uint64(1); v <= c.n; v++ {
			publish(ctx, js, rec, v)
		}
	}
	return finalRead(ctx, stream, rec)
}

// connectHelper opens the consumer's connection. In a proxy scenario it
// first sets up a clean helper-line in toxiproxy, then connects through it.
//
// nats.go reports some problems in the background instead of returning
// them, so the connection gets an error handler that records them. A slow
// consumer error is also the proof that the overflow fault happened: the
// client's pending tray filled up and it dropped messages. The connection
// also records when it drops and when it comes back, and with
// -block-disconnect its disconnect handler then blocks, the way a slow
// application handler would.
func connectHelper(c config, sc scenario, rec *recorder) (*nats.Conn, error) {
	url := c.url
	if sc.proxy {
		tp := toxiproxy{api: c.toxiproxy}
		if err := tp.setUp(); err != nil {
			return nil, err
		}
		url = c.helperURL
	}
	var overflowed sync.Once
	// These are connection-level events, not any one subscription's. With a
	// single subscription the name stays "consumer" so existing histories do
	// not change; with more it would be a phantom consumer that never
	// delivers anything.
	clientName := "consumer"
	if subCount(c, sc) > 1 {
		clientName = "client"
	}
	onError := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		rec.op(op{Process: clientName, Type: "info", F: "client-error", Error: err.Error()})
		if errors.Is(err, nats.ErrSlowConsumer) {
			overflowed.Do(func() {
				rec.op(op{Process: "nemesis", Type: "ok", F: "overflow"})
			})
		}
	}
	onDisconnect := func(_ *nats.Conn, err error) {
		// Close reports a nil error; only a dropped connection is news.
		if err != nil {
			rec.op(op{Process: clientName, Type: "info", F: "disconnect", Error: err.Error()})
			// -block-disconnect plays a slow application handler; 0 doesn't
			// sleep. nats.go runs a connection's callbacks one at a time,
			// so anything it queues behind this one waits too.
			time.Sleep(c.blockDisconnect)
		}
	}
	onReconnect := func(_ *nats.Conn) {
		rec.op(op{Process: clientName, Type: "info", F: "reconnect"})
	}
	opts := []nats.Option{
		nats.ErrorHandler(onError),
		nats.DisconnectErrHandler(onDisconnect),
		nats.ReconnectHandler(onReconnect),
	}
	if sc.subDeath {
		// repro2201's settings, and they are the fault. With the default
		// two-second ReconnectWait the whole reconnect loop runs while the
		// connection is already down, so the client comes back once and
		// recreates every consumer with nothing competing: no create is ever
		// in flight when the next disconnect lands, and the bug cannot appear.
		opts = append(opts, nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	}
	hc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting the consumer to %s: %w", url, err)
	}
	return hc, nil
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

// consume runs an ordered consumer through the scenario. The consumer reads
// on hc, its own connection; the producer publishes through js, on the
// other one. Only the first message is published before it subscribes. The
// rest follow once that first message has arrived, which is where a stall
// scenario's callback blocks, so the flood always lands after the trap is
// armed. Shrinking the tray first matters: it only turns away new arrivals,
// so a backlog that arrived earlier would never overflow it. A network or
// server fault starts halfway through the flood, so it hits a consumer
// that is busy receiving. One more message follows once the consumer has
// caught up, and it stops when it has seen everything or the grace window
// runs out.
func consume(ctx context.Context, hc *nats.Conn, js jetstream.JetStream, rec *recorder, c config, sc scenario) error {
	gate := make(chan struct{})
	var stalled sync.Once
	// One subscription is the default and keeps the process name "consumer",
	// so every existing scenario's history is unchanged. Only sub-death opens
	// more, because telling a dead subscription from a slow one needs a
	// sibling that is still receiving.
	subs := make([]*subscription, subCount(c, sc))
	for i := range subs {
		name := "consumer"
		if len(subs) > 1 {
			name = fmt.Sprintf("consumer-%d", i+1)
		}
		subs[i] = &subscription{name: name}
	}
	// Each subscription records under its own name. Its reached counter is
	// touched only atomically: waitForAll reads it from this goroutine, and
	// onMsg can run on more than one, because the jetstream consumer starts a
	// new subscription every time it resets.
	handlers := func(s *subscription, first bool) (func([]byte, uint64), func(error)) {
		onErr := func(err error) {
			rec.op(op{Process: s.name, Type: "info", F: "client-error", Error: err.Error()})
		}
		onMsg := func(data []byte, seq uint64) {
			v, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				onErr(err)
				return
			}
			rec.op(op{Process: s.name, Type: "ok", F: "deliver", Value: v, StreamSeq: seq})
			raise(&s.reached, seq)
			// The stall models one blocked callback. Blocking every
			// subscription would just be a stopped consumer.
			if sc.stall > 0 && first {
				stalled.Do(func() { <-gate })
			}
		}
		return onMsg, onErr
	}

	// start-new publishes a prefix the consumer must never see, then subscribes
	// past it. Nothing is delivered before the reconnect below, which is the
	// precondition for a reset that forgets its start position and recreates the
	// consumer from the beginning of the stream.
	firstRun := uint64(2)
	var last uint64
	if sc.startNew {
		for v := uint64(1); v <= c.prefix; v++ {
			last = publish(ctx, js, rec, v)
		}
		firstRun = c.prefix + 1
	} else {
		last = publish(ctx, js, rec, 1)
	}
	for i, s := range subs {
		onMsg, onErr := handlers(s, i == 0)
		rec.op(op{Process: s.name, Type: "invoke", F: "subscribe"})
		var err error
		if c.api == "jetstream" {
			s.rd, err = subscribeJetStream(ctx, hc, c, sc, onMsg, onErr)
		} else {
			s.rd, err = subscribeLegacy(hc, c, sc, onMsg, onErr)
		}
		if err != nil {
			rec.op(op{Process: s.name, Type: "info", F: "subscribe", Error: err.Error()})
			return err
		}
		rec.op(op{Process: s.name, Type: "ok", F: "subscribe"})
	}
	if sc.startNew {
		// Waiting for a delivery would burn the whole grace: a correct consumer
		// has nothing to deliver yet, and that is exactly the state the reset
		// mishandles.
		rec.op(op{Process: "nemesis", Type: "invoke", F: "reconnect"})
		if err := hc.ForceReconnect(); err != nil {
			return err
		}
		waitConnected(hc, c.grace)
		rec.op(op{Process: "nemesis", Type: "ok", F: "reconnect"})
	} else {
		waitForAll(subs, last, c.grace)
	}

	// sub-death drops and remakes the connection repeatedly, with a gap
	// shorter than a consumer create, so a disconnect lands while the reset
	// from the previous one is still in flight. That is the case that used to
	// kill a push ordered subscription for good, with no error raised to the
	// caller. See drivers/natsgo/repro2201.
	if sc.subDeath {
		rec.op(op{Process: "nemesis", Type: "invoke", F: "reconnect"})
		for range c.reconnects {
			if err := hc.ForceReconnect(); err != nil {
				return err
			}
			time.Sleep(c.reconnectGap)
		}
		waitConnected(hc, c.grace)
		rec.op(op{Process: "nemesis", Type: "ok", F: "reconnect"})
	}

	// The 2107 trigger: a blocked callback while the flood arrives behind it,
	// and, where the API has one, a tiny pending tray for the flood to overflow.
	if sc.stall > 0 {
		if subs[0].rd.shrinkTray != nil {
			rec.op(op{Process: "nemesis", Type: "invoke", F: "overflow"})
			if err := subs[0].rd.shrinkTray(); err != nil {
				return err
			}
		}
		rec.op(op{Process: "nemesis", Type: "invoke", F: "stall"})
	}
	tp := toxiproxy{api: c.toxiproxy}
	netStarted := false
	var serverDone chan error
	for v := firstRun; v <= c.n; v++ {
		if sc.net != "" && v == c.n/2+1 {
			if err := startNetFault(tp, rec, sc); err != nil {
				return err
			}
			netStarted = true
		}
		// A server fault starts right away, but it ends on its own timer,
		// because the publishes below keep waiting on a server that can't
		// answer.
		if sc.server != "" && v == c.n/2+1 {
			finish, err := startServerFault(ctx, js, rec, c, sc)
			if err != nil {
				return err
			}
			serverDone = make(chan error, 1)
			go func() { serverDone <- finish() }()
		}
		last = max(last, publish(ctx, js, rec, v))
	}
	if sc.stall > 0 {
		time.Sleep(sc.stall)
		close(gate)
		rec.op(op{Process: "nemesis", Type: "ok", F: "stall"})
	}
	if netStarted {
		time.Sleep(sc.hold)
		if err := endNetFault(tp, rec, sc, hc); err != nil {
			return err
		}
	}
	if serverDone != nil {
		if err := <-serverDone; err != nil {
			return err
		}
	}

	waitForAll(subs, last, c.grace)
	last = max(last, publish(ctx, js, rec, c.n+1))
	waitForAll(subs, last, c.grace)
	// Stop them all before reporting, so one bad stop can't hide the rest.
	var stopErr error
	for _, s := range subs {
		if err := s.rd.stop(); err != nil && stopErr == nil {
			stopErr = err
		}
	}
	return stopErr
}

// startNetFault breaks the consumer's line in the middle of the flood:
// "slow" makes everything the server sends the consumer arrive slowLatency
// late, and "cut" drops the connection and refuses new ones.
func startNetFault(tp toxiproxy, rec *recorder, sc scenario) error {
	rec.op(op{Process: "nemesis", Type: "invoke", F: sc.net})
	if sc.net == "slow" {
		return tp.slow(slowLatency)
	}
	return tp.cut()
}

// endNetFault heals the line. A slow line is done once toxiproxy has taken
// the toxic back. A cut only counts if the consumer's client really lost its
// connection: if it never noticed, the fault never happened, and the run is
// invalid rather than clean.
func endNetFault(tp toxiproxy, rec *recorder, sc scenario, hc *nats.Conn) error {
	lost := !hc.IsConnected()
	if err := tp.heal(); err != nil {
		return err
	}
	if sc.net == "cut" && !lost {
		return nil
	}
	rec.op(op{Process: "nemesis", Type: "ok", F: sc.net})
	return nil
}

// startServerFault freezes or kills nats-server right away, so the rest of
// the flood really meets a server that can't answer, and returns finish,
// which brings it back after sc.hold. finish runs on its own goroutine,
// because the producer keeps publishing meanwhile and each of those
// publishes waits for an ack. The fault only counts as done if docker
// shows it really happened: a paused container, or one that exited with
// code 137, which means it was killed, and came back as a server with a
// new ID, because nats-server picks a fresh one every time it starts.
func startServerFault(ctx context.Context, js jetstream.JetStream, rec *recorder, c config, sc scenario) (func() error, error) {
	rec.op(op{Process: "nemesis", Type: "invoke", F: sc.server})
	if sc.server == "pause" {
		if _, err := docker("pause", c.container); err != nil {
			return nil, err
		}
		paused, checkErr := docker("inspect", "-f", "{{.State.Paused}}", c.container)
		return func() error {
			time.Sleep(sc.hold)
			// Unpause whatever the check said, so a run never leaves the
			// server frozen.
			if _, err := docker("unpause", c.container); err != nil {
				return err
			}
			if checkErr != nil {
				return checkErr
			}
			if paused == "true" {
				rec.op(op{Process: "nemesis", Type: "ok", F: sc.server})
			}
			return nil
		}, nil
	}

	before := js.Conn().ConnectedServerId()
	if _, err := docker("kill", c.container); err != nil {
		return nil, err
	}
	code, waitErr := docker("wait", c.container)
	return func() error {
		time.Sleep(sc.hold)
		if _, err := docker("start", c.container); err != nil {
			return err
		}
		if waitErr != nil {
			return waitErr
		}
		if err := waitForJetStream(ctx, js, time.Minute); err != nil {
			return err
		}
		if code == "137" && js.Conn().ConnectedServerId() != before {
			rec.op(op{Process: "nemesis", Type: "ok", F: sc.server})
		}
		return nil
	}, nil
}

// waitForJetStream asks for the stream's info until the restarted server
// answers, and gives up after limit.
func waitForJetStream(ctx context.Context, js jetstream.JetStream, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		askCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := js.Stream(askCtx, streamName)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("JetStream didn't come back within %s: %w", limit, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// docker runs one docker command and returns what it printed. It gives up
// after 30 seconds, so a docker daemon that hangs can't hang the run.
func docker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return strings.TrimSpace(string(out)), nil
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
func subscribeLegacy(hc *nats.Conn, c config, sc scenario, onMsg func([]byte, uint64), onErr func(error)) (reader, error) {
	legacy, err := hc.JetStream()
	if err != nil {
		return reader{}, err
	}
	// start-new asks to begin past the prefix, so the consumer starts with
	// nothing delivered and no position of its own to resume from.
	opts := []nats.SubOpt{nats.OrderedConsumer()}
	if sc.startNew {
		opts = append(opts, nats.DeliverNew())
	}
	sub, err := legacy.Subscribe(subject, func(m *nats.Msg) {
		md, err := m.Metadata()
		if err != nil {
			onErr(err)
			return
		}
		onMsg(m.Data, md.Sequence.Stream)
	}, opts...)
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
func subscribeJetStream(ctx context.Context, hc *nats.Conn, c config, sc scenario, onMsg func([]byte, uint64), onErr func(error)) (reader, error) {
	hjs, err := jetstream.New(hc)
	if err != nil {
		return reader{}, err
	}
	oc := jetstream.OrderedConsumerConfig{}
	if sc.startNew {
		oc.DeliverPolicy = jetstream.DeliverNewPolicy
	}
	cons, err := hjs.OrderedConsumer(ctx, streamName, oc)
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

// waitConnected polls until the connection is back or limit runs out. The
// reset this scenario is about runs on CONNECTED, so the publishes must not
// start before it.
func waitConnected(nc *nats.Conn, limit time.Duration) {
	deadline := time.Now().Add(limit)
	for !nc.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// subscription is one consumer's own state: the process name its operations
// carry, how far it has got, and its reader.
type subscription struct {
	name    string
	reached atomic.Uint64
	rd      reader
}

// subCount is how many subscriptions to open: the flag when it is set, then
// the scenario's own default, then one.
func subCount(c config, sc scenario) int {
	if c.subs > 0 {
		return c.subs
	}
	if sc.subs > 0 {
		return sc.subs
	}
	return 1
}

// waitForAll polls until every subscription has reached target or grace runs
// out. A dead one never reaches it, so a run with deaths waits out the whole
// grace. That is the point: the checker needs evidence that the subscription
// had the time and still received nothing.
func waitForAll(subs []*subscription, target uint64, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		behind := false
		for _, s := range subs {
			if s.reached.Load() < target {
				behind = true
				break
			}
		}
		if !behind {
			return
		}
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
	StartSeq uint64   `json:"start_seq,omitempty"`
	Subs     int      `json:"subs,omitempty"`
	Driftlab string   `json:"driftlab"`
}

type params struct {
	N               uint64 `json:"n"`
	PendingMsgs     int    `json:"pending_msgs,omitempty"`
	Stall           string `json:"stall,omitempty"`
	Via             string `json:"via,omitempty"`
	Hold            string `json:"hold,omitempty"`
	Latency         string `json:"latency,omitempty"`
	BlockDisconnect string `json:"block_disconnect,omitempty"`
	Reconnects      int    `json:"reconnects,omitempty"`
	ReconnectGap    string `json:"reconnect_gap,omitempty"`
}

// newMeta describes the run. A stall scenario declares the stall, and for
// the legacy API also the overflow it is meant to cause. A network or
// server scenario declares its fault (slow, cut, pause or kill) and how
// long it lasts. If a declared fault never happens, the checker calls the
// run invalid instead of clean. A proxy scenario says its consumer's line
// ran through toxiproxy, and -block-disconnect says how long the consumer's
// disconnect handler blocked.
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
		if c.blockDisconnect > 0 {
			m.Params.BlockDisconnect = c.blockDisconnect.String()
		}
	}
	if sc.proxy {
		m.Params.Via = "toxiproxy"
	}
	if sc.stall > 0 {
		m.Faults = []string{"stall"}
		if c.api == "legacy" {
			m.Faults = append(m.Faults, "overflow")
		}
		m.Params.PendingMsgs = c.pending
		m.Params.Stall = sc.stall.String()
	}
	if sc.net != "" {
		m.Faults = []string{sc.net}
		m.Params.Hold = sc.hold.String()
		if sc.net == "slow" {
			m.Params.Latency = slowLatency.String()
		}
	}
	if sc.server != "" {
		m.Faults = []string{sc.server}
		m.Params.Hold = sc.hold.String()
	}
	if sc.startNew {
		// The stream is fresh, so the prefix occupies stream_seq 1..prefix and
		// the consumer's first wanted message is prefix+1.
		m.Faults = []string{"reconnect"}
		m.StartSeq = c.prefix
	}
	if sc.subDeath {
		m.Faults = []string{"reconnect"}
		m.Params.Reconnects = c.reconnects
		m.Params.ReconnectGap = c.reconnectGap.String()
	}
	if sc.consumer {
		m.Subs = subCount(c, sc)
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
		if dep.Path != "github.com/nats-io/nats.go" {
			continue
		}
		client = dep.Path + "@" + dep.Version
		// A build that swaps in another copy of nats.go, such as a pull
		// request's branch, says which: Go reports the swap separately, and
		// the version above would still name the release it replaced.
		if r := dep.Replace; r != nil {
			client += " => " + r.Path
			if r.Version != "" {
				client += "@" + r.Version
			}
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

// toxiproxyClient talks to toxiproxy's HTTP API. The timeout stops a box
// that hangs from hanging the whole run.
var toxiproxyClient = &http.Client{Timeout: 10 * time.Second}

// toxiproxy drives the bad-line box through its HTTP API.
type toxiproxy struct {
	api string
}

// setUp gives the consumer a clean line. It creates helper-line if it
// doesn't exist yet, then heals it.
func (t toxiproxy) setUp() error {
	proxies := []map[string]any{{
		"name":     proxyName,
		"listen":   proxyListen,
		"upstream": proxyUpstream,
		"enabled":  true,
	}}
	if err := t.call(http.MethodPost, "/populate", proxies); err != nil {
		return err
	}
	return t.heal()
}

// slow makes everything the server sends the consumer arrive latency late.
func (t toxiproxy) slow(latency time.Duration) error {
	return t.call(http.MethodPost, "/proxies/"+proxyName+"/toxics", map[string]any{
		"name":       "slow",
		"type":       "latency",
		"stream":     "downstream",
		"attributes": map[string]any{"latency": latency.Milliseconds()},
	})
}

// cut disables helper-line: toxiproxy drops the consumer's connection and
// refuses new ones until the line is healed.
func (t toxiproxy) cut() error {
	return t.call(http.MethodPost, "/proxies/"+proxyName, map[string]any{"enabled": false})
}

// heal enables helper-line again and removes every toxic, including any an
// earlier run left behind.
func (t toxiproxy) heal() error {
	return t.call(http.MethodPost, "/reset", nil)
}

// call sends one request to the API and fails unless the answer is 2xx.
func (t toxiproxy) call(method, path string, body any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, t.api+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := toxiproxyClient.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy %s %s: %w (is it up? docker compose -f deploy/compose.yaml up -d)", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("toxiproxy %s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}
