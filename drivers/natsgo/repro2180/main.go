// Command repro2180 measures how long a consumer stays silent after its
// connection to nats-server is cut for longer than 5 seconds. It covers the
// push ordered consumer, the KV watchers that both APIs build on it, and the
// jetstream package's ordered consumer as a control.
// See https://github.com/nats-io/nats.go/issues/2180.
//
// It needs a nats-server with JetStream (-server). The consumer connects
// through a small in-process TCP proxy that can cut its connection. A second,
// direct connection puts a new value into a KV bucket every 100ms and polls
// how many consumers the bucket's stream has.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	bucket  = "repro2180"
	key     = "tick"
	stream  = "KV_" + bucket
	subject = "$KV." + bucket + "." + key
)

var cases = []string{"push-ordered", "kv-legacy", "kv-jetstream", "pull-ordered"}

func main() {
	server := flag.String("server", "127.0.0.1:4222", "host:port of a nats-server with JetStream")
	cut := flag.Duration("cut", 8*time.Second, "how long the consumer's connection stays cut")
	only := flag.String("case", "", "run only this case: push-ordered, kv-legacy, kv-jetstream or pull-ordered")
	flag.Parse()

	names := cases
	if *only != "" {
		names = []string{*only}
	}
	silent := make([]time.Duration, len(names))
	for i, name := range names {
		d, err := run(name, *server, *cut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
		silent[i] = d
	}

	fmt.Printf("\nsilent after a %s cut ended:\n", *cut)
	for i, name := range names {
		fmt.Printf("  %-13s %6.2fs\n", name, silent[i].Seconds())
	}
}

// run measures one case and returns how long its consumer stayed silent
// after the cut ended.
func run(name, server string, cut time.Duration) (time.Duration, error) {
	fmt.Printf("\n== %s ==\n", name)
	start := time.Now()
	logf := func(format string, a ...any) {
		fmt.Printf("%6.2fs  %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, a...))
	}
	ctx := context.Background()

	// The direct connection, and a fresh bucket.
	direct, err := nats.Connect("nats://" + server)
	if err != nil {
		return 0, err
	}
	defer direct.Close()
	js, err := jetstream.New(direct)
	if err != nil {
		return 0, err
	}
	err = js.DeleteKeyValue(ctx, bucket)
	if err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		return 0, err
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket})
	if err != nil {
		return 0, err
	}

	// The consumer's connection, through the proxy.
	line, err := newBadLine(server)
	if err != nil {
		return 0, err
	}
	defer line.shutdown()
	nc, err := nats.Connect("nats://"+line.addr(),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil { // nil when we close the connection ourselves
				logf("consumer disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) { logf("consumer reconnected") }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logf("consumer error: %v", err)
		}),
	)
	if err != nil {
		return 0, err
	}
	defer nc.Close()

	revs, stop, err := follow(ctx, nc, name)
	if err != nil {
		return 0, err
	}
	defer stop()
	logf("consumer started")

	stopPuts := every(100*time.Millisecond, func() {
		if _, err := kv.Put(ctx, key, []byte(time.Now().Format(time.RFC3339Nano))); err != nil {
			logf("put failed: %v", err)
		}
	})
	defer stopPuts()
	consumers := -1
	stopPolls := every(250*time.Millisecond, func() {
		s, err := js.Stream(ctx, stream)
		if err != nil {
			return
		}
		if n := s.CachedInfo().State.Consumers; n != consumers {
			consumers = n
			logf("the bucket's stream has %d consumer(s)", n)
		}
	})
	defer stopPolls()

	last := drain(revs, 2*time.Second)
	logf("cutting the connection (last revision seen: %d)", last)
	line.cut()
	drain(revs, cut)
	logf("letting connections through again")
	line.heal()
	back := time.Now()

	select {
	case rev := <-revs:
		silent := time.Since(back)
		logf("first revision after the cut: %d", rev)
		return silent, nil
	case <-time.After(time.Minute):
		return 0, errors.New("nothing arrived within a minute of the cut ending")
	}
}

// follow starts the named consumer on nc. It sends the stream sequence of
// every message it gets, which is the KV revision, on the returned channel,
// and returns a func that stops the consumer.
func follow(ctx context.Context, nc *nats.Conn, name string) (<-chan uint64, func(), error) {
	revs := make(chan uint64, 1024)
	switch name {
	case "push-ordered": // the consumer the issue is about
		js, err := nc.JetStream()
		if err != nil {
			return nil, nil, err
		}
		sub, err := js.Subscribe(subject, func(m *nats.Msg) {
			if md, err := m.Metadata(); err == nil {
				revs <- md.Sequence.Stream
			}
		}, nats.OrderedConsumer())
		if err != nil {
			return nil, nil, err
		}
		return revs, func() { sub.Unsubscribe() }, nil

	case "kv-legacy": // a watcher built on the push ordered consumer
		js, err := nc.JetStream()
		if err != nil {
			return nil, nil, err
		}
		kv, err := js.KeyValue(bucket)
		if err != nil {
			return nil, nil, err
		}
		w, err := kv.Watch(key)
		if err != nil {
			return nil, nil, err
		}
		go func() {
			for e := range w.Updates() {
				if e != nil { // nil marks the end of the initial values
					revs <- e.Revision()
				}
			}
		}()
		return revs, func() { w.Stop() }, nil

	case "kv-jetstream": // built on the push ordered consumer too
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, nil, err
		}
		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			return nil, nil, err
		}
		w, err := kv.Watch(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		go func() {
			for e := range w.Updates() {
				if e != nil {
					revs <- e.Revision()
				}
			}
		}()
		return revs, func() { w.Stop() }, nil

	case "pull-ordered": // the control
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, nil, err
		}
		c, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
		})
		if err != nil {
			return nil, nil, err
		}
		cc, err := c.Consume(func(m jetstream.Msg) {
			if md, err := m.Metadata(); err == nil {
				revs <- md.Sequence.Stream
			}
		})
		if err != nil {
			return nil, nil, err
		}
		return revs, cc.Stop, nil
	}
	return nil, nil, fmt.Errorf("unknown case %q", name)
}

// drain reads revisions for d, so the consumer never waits on us, and
// returns the last one.
func drain(revs <-chan uint64, d time.Duration) uint64 {
	var last uint64
	end := time.After(d)
	for {
		select {
		case rev := <-revs:
			last = rev
		case <-end:
			return last
		}
	}
}

// every calls f every d on its own goroutine until the returned func is
// called. That func waits for a call in progress to finish.
func every(d time.Duration, f func()) (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		tick := time.NewTicker(d)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				f()
			}
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

// badLine is a TCP proxy to the server. cut closes every connection through
// it and turns new ones away until heal, like disabling a toxiproxy proxy.
type badLine struct {
	ln     net.Listener
	server string

	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

// newBadLine starts a proxy to server on a free local port.
func newBadLine(server string) (*badLine, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &badLine{ln: ln, server: server}
	go l.serve()
	return l, nil
}

func (l *badLine) addr() string { return l.ln.Addr().String() }

func (l *badLine) serve() {
	for {
		client, err := l.ln.Accept()
		if err != nil {
			return // the listener was closed
		}
		upstream, err := net.Dial("tcp", l.server)
		if err != nil {
			client.Close()
			continue
		}
		l.mu.Lock()
		if l.down {
			client.Close()
			upstream.Close()
		} else {
			l.conns = append(l.conns, client, upstream)
			go pipe(upstream, client)
			go pipe(client, upstream)
		}
		l.mu.Unlock()
	}
}

// pipe copies src to dst until either side closes, then closes both.
func pipe(dst, src net.Conn) {
	io.Copy(dst, src)
	dst.Close()
	src.Close()
}

func (l *badLine) cut() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = true
	for _, c := range l.conns {
		c.Close()
	}
	l.conns = nil
}

func (l *badLine) heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = false
}

func (l *badLine) shutdown() {
	l.ln.Close()
	l.cut()
}
