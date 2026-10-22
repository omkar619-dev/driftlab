// Command repro2201 counts how many push ordered subscriptions survive repeated
// reconnects.
//
// Recreating the ordered consumer on every reconnect means a second disconnect
// can land while the create is still in flight. That create returns
// ErrDisconnected, which was not on the retry list, so pushErr unsubscribed the
// subscription for good: it is gone until the process restarts, with no further
// delivery and no way back. One short connection blip is enough.
// See https://github.com/nats-io/nats.go/pull/2201.
//
// It needs a nats-server with JetStream (-server). Reconnects are driven with
// Conn.ForceReconnect rather than a proxy, so the timing is the client's own and
// no network fault is needed.
//
// A run reports how many subscriptions are still valid at the end and how many
// received a marker published afterwards. Both should equal -subs. Exits 1 if
// any subscription died.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName = "REPRO2201"
	subject    = "repro2201.msg"
)

func main() {
	server := flag.String("server", "127.0.0.1:4222", "host:port of a nats-server with JetStream")
	subs := flag.Int("subs", 300, "how many push ordered subscriptions to open")
	rounds := flag.Int("rounds", 10, "how many times to force a reconnect")
	gap := flag.Duration("gap", 100*time.Millisecond, "wait between reconnects; shorter than a consumer create is what lands a disconnect mid-create")
	settle := flag.Duration("settle", 5*time.Second, "how long to let the last reconnect finish before the marker")
	wait := flag.Duration("wait", 5*time.Second, "how long to wait for the marker to arrive")
	flag.Parse()

	if err := run(*server, *subs, *rounds, *gap, *settle, *wait); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
}

func run(server string, nSubs, rounds int, gap, settle, wait time.Duration) error {
	// Errors that kill a subscription arrive here rather than at the call
	// site, so they are the only record of why one went away.
	var errMu sync.Mutex
	errCounts := map[string]int{}
	note := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		errCounts[err.Error()]++
		errMu.Unlock()
	}

	nc, err := nats.Connect("nats://"+server,
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { note(err) }),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(10*time.Millisecond),
	)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := freshStream(ctx, js); err != nil {
		return fmt.Errorf("stream: %w", err)
	}

	// The legacy JetStreamContext is the API that carries the push ordered
	// consumer, which is the one this is about.
	legacy, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("legacy jetstream: %w", err)
	}

	// marked[i] counts what subscription i received after the marker was
	// published. A dead subscription stays at zero.
	marked := make([]atomic.Uint64, nSubs)
	handles := make([]*nats.Subscription, 0, nSubs)
	var markerSent atomic.Bool

	for i := range nSubs {
		idx := i
		sub, err := legacy.Subscribe(subject, func(*nats.Msg) {
			if markerSent.Load() {
				marked[idx].Add(1)
			}
		}, nats.OrderedConsumer())
		if err != nil {
			return fmt.Errorf("subscribe %d of %d: %w", i+1, nSubs, err)
		}
		handles = append(handles, sub)
	}
	defer func() {
		for _, s := range handles {
			_ = s.Unsubscribe()
		}
	}()

	fmt.Printf("opened %d push ordered subscriptions\n", len(handles))

	// Each ForceReconnect drops and re-establishes the connection. With a gap
	// shorter than a consumer create, the next drop lands while the reset from
	// the previous one is still in flight, which is the case that used to be
	// terminal.
	for r := 1; r <= rounds; r++ {
		if err := nc.ForceReconnect(); err != nil {
			return fmt.Errorf("force reconnect %d: %w", r, err)
		}
		time.Sleep(gap)
	}
	fmt.Printf("forced %d reconnects, %v apart\n", rounds, gap)

	time.Sleep(settle)

	valid := 0
	for _, s := range handles {
		if s.IsValid() {
			valid++
		}
	}

	// A subscription can be valid and still not deliver, so the marker is the
	// stronger check of the two.
	markerSent.Store(true)
	if _, err := js.Publish(ctx, subject, []byte("marker")); err != nil {
		return fmt.Errorf("publish marker: %w", err)
	}
	time.Sleep(wait)

	got := 0
	for i := range handles {
		if marked[i].Load() > 0 {
			got++
		}
	}

	fmt.Printf("\nsubs=%d still-valid=%d got-marker=%d died=%d\n",
		nSubs, valid, got, nSubs-got)

	errMu.Lock()
	kinds := make([]string, 0, len(errCounts))
	for k := range errCounts {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(a, b int) bool { return errCounts[kinds[a]] > errCounts[kinds[b]] })
	if len(kinds) > 0 {
		fmt.Println("\nasync errors:")
		for _, k := range kinds {
			fmt.Printf("  %4d  %s\n", errCounts[k], k)
		}
	}
	errMu.Unlock()

	if got < nSubs {
		fmt.Printf("\nFAIL: %d of %d subscriptions stopped delivering\n", nSubs-got, nSubs)
		os.Exit(1)
	}
	fmt.Printf("\nOK: all %d subscriptions still delivering\n", nSubs)
	return nil
}

// freshStream drops any stream left by an earlier run so each run starts from
// sequence 1 with no consumers.
func freshStream(ctx context.Context, js jetstream.JetStream) error {
	err := js.DeleteStream(ctx, streamName)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) &&
		!strings.Contains(err.Error(), "stream not found") {
		return err
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.MemoryStorage,
	})
	return err
}
