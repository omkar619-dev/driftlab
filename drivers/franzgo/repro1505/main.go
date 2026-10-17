// Command repro1505 reproduces https://github.com/twmb/franz-go/issues/1505:
// a franz-go request whose context ends while it waits behind another
// request to the same broker, whose connection attempt hangs, doesn't return
// until that attempt gives up.
//
// The client's dialer stands in for a broker that never answers: it blocks
// for 10 seconds, franz-go's default dial timeout, unless the caller's
// context ends first, as net.Dialer.DialContext does. Nothing here touches
// the network.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hang is the dialer: a broker that accepts no connections.
func hang(ctx context.Context, _, _ string) (net.Conn, error) {
	select {
	case <-time.After(10 * time.Second):
		return nil, errors.New("dial timed out after 10s")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newClient() *kgo.Client {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:9092"), kgo.Dialer(hang))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return cl
}

// request sends one metadata request with the given deadline and prints how
// long it took to come back, and when, counted from start.
func request(cl *kgo.Client, name string, deadline time.Duration, start time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	sent := time.Now()
	_, err := cl.Request(ctx, kmsg.NewPtrMetadataRequest())
	fmt.Printf("%-22s deadline %-4s returned after %5.2fs, at %5.2fs: %v\n", name, deadline, time.Since(sent).Seconds(), time.Since(start).Seconds(), err)
}

func main() {
	fmt.Println("franz-go", version())

	// Alone, a request with a 1-second deadline comes back at its deadline.
	cl := newClient()
	request(cl, "alone", time.Second, time.Now())
	cl.Close()

	// The same request, sent while another request to the same broker is
	// still trying to connect.
	cl = newClient()
	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		request(cl, "first, 15s deadline", 15*time.Second, start)
	}()
	time.Sleep(200 * time.Millisecond)
	request(cl, "second, behind it", time.Second, start)
	<-done
	cl.Close()
}

// version returns the franz-go version this was built with.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/twmb/franz-go" {
			return dep.Version
		}
	}
	return "unknown"
}
