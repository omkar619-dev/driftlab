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
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName = "DRIFTLAB"
	subject    = "driftlab"
)

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server to run against")
	n := flag.Uint64("n", 100, "number of messages to publish")
	flag.Parse()
	if err := run(*url, *n, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "natsdriver:", err)
		os.Exit(1)
	}
}

func run(url string, n uint64, out io.Writer) error {
	nc, err := nats.Connect(url)
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
	rec.writeMeta(nc.ConnectedServerVersion(), "publish-only")
	for v := uint64(1); v <= n; v++ {
		rec.op(op{Process: "producer", Type: "invoke", F: "publish", Value: v})
		ack, err := js.Publish(ctx, subject, []byte(strconv.FormatUint(v, 10)))
		if err != nil {
			rec.op(op{Process: "producer", Type: "info", F: "publish", Value: v, Error: err.Error()})
			continue
		}
		rec.op(op{Process: "producer", Type: "ok", F: "publish", Value: v, StreamSeq: ack.Sequence})
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
	return rec.err
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
	Server   string `json:"server"`
	Scenario string `json:"scenario"`
	Driftlab string `json:"driftlab"`
}

type recorder struct {
	enc   *json.Encoder
	start time.Time
	next  int
	err   error
}

func newRecorder(w io.Writer) *recorder {
	return &recorder{enc: json.NewEncoder(w), start: time.Now()}
}

func (r *recorder) writeMeta(server, scenario string) {
	client, rev := provenance()
	r.err = r.enc.Encode(metaLine{
		Type:     "meta",
		Driver:   "natsgo",
		Client:   client,
		Server:   "nats-server " + server,
		Scenario: scenario,
		Driftlab: rev,
	})
}

// op writes one operation. After the first write error it writes nothing
// more; finalRead returns that error at the end of the run.
func (r *recorder) op(o op) {
	if r.err != nil {
		return
	}
	o.Index = r.next
	o.TimeNS = time.Since(r.start).Nanoseconds()
	r.next++
	r.err = r.enc.Encode(o)
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
