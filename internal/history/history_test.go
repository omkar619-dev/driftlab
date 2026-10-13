package history

import (
	"strings"
	"testing"
	"time"
)

const meta = `{"type":"meta","driver":"natsgo","client":"github.com/nats-io/nats.go@v1.53.1","api":"legacy","scenario":"control"}`

func lines(ls ...string) string { return strings.Join(ls, "\n") + "\n" }

func TestParseValid(t *testing.T) {
	in := lines(
		meta,
		`{"index":0,"time_ns":0,"process":"producer","type":"invoke","f":"publish","value":1}`,
		"",
		`{"index":1,"time_ns":410000,"process":"producer","type":"ok","f":"publish","value":1,"stream_seq":1,"added_later":true}`,
	)
	h, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if h.Meta.Client != "github.com/nats-io/nats.go@v1.53.1" {
		t.Errorf("Meta.Client = %q", h.Meta.Client)
	}
	if len(h.Ops) != 2 {
		t.Fatalf("got %d ops, want 2", len(h.Ops))
	}
	if got := h.Ops[1]; got.Type != OK || got.StreamSeq != 1 || got.Time != 410*time.Microsecond {
		t.Errorf("Ops[1] = %+v", got)
	}
	if h.Kafka {
		t.Error("Kafka = true for a history numbered by stream_seq")
	}
}

// TestParsePositions checks both ways of numbering messages, and that a
// Kafka offset of 0 is a real position rather than a missing one.
func TestParsePositions(t *testing.T) {
	tests := []struct {
		name   string
		op     string
		want   Pos
		placed bool
		kafka  bool
	}{
		{"stream_seq", `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"stream_seq":7}`, Pos{Offset: 7}, true, false},
		{"offset 0 is real", `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"partition":0,"offset":0}`, Pos{}, true, true},
		{"partition and offset", `{"index":0,"process":"producer","type":"ok","f":"publish","value":1,"partition":2,"offset":5}`, Pos{Partition: 2, Offset: 5}, true, true},
		{"no position", `{"index":0,"process":"producer","type":"info","f":"publish","value":1}`, Pos{}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := Parse(strings.NewReader(lines(meta, tt.op)))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, placed := h.Ops[0].Pos()
			if got != tt.want || placed != tt.placed || h.Kafka != tt.kafka {
				t.Errorf("Pos() = %+v, %v with Kafka %v; want %+v, %v with Kafka %v", got, placed, h.Kafka, tt.want, tt.placed, tt.kafka)
			}
		})
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty file", "", "missing the meta line"},
		{"first line is not meta", lines(`{"index":0,"process":"producer","type":"invoke","f":"publish","value":1}`), `want "meta"`},
		{"meta without client", lines(`{"type":"meta","scenario":"control"}`), "client and scenario"},
		{"broken json", lines(meta, `{"index":0,`), "line 2"},
		{"index gap", lines(meta, `{"index":1,"process":"producer","type":"invoke","f":"publish","value":1}`), "index is 1, want 0"},
		{"unknown type", lines(meta, `{"index":0,"process":"producer","type":"maybe","f":"publish","value":1}`), `unknown type "maybe"`},
		{"ok publish without stream_seq", lines(meta, `{"index":0,"process":"producer","type":"ok","f":"publish","value":1}`), "stream_seq"},
		{"deliver that is not ok", lines(meta, `{"index":0,"process":"consumer","type":"info","f":"deliver","value":1,"stream_seq":1}`), "must have type ok"},
		{"negative value", lines(meta, `{"index":0,"process":"producer","type":"invoke","f":"publish","value":-1}`), "line 2"},
		{"partition without offset", lines(meta, `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"partition":0}`), "go together"},
		{"offset without partition", lines(meta, `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"offset":0}`), "go together"},
		{"stream_seq and offset on one op", lines(meta, `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"stream_seq":1,"partition":0,"offset":0}`), "a position is"},
		{"read without a position", lines(meta, `{"index":0,"process":"final-read","type":"ok","f":"read","value":1}`), "needs a value and a position"},
		{"both numberings in one history", lines(meta,
			`{"index":0,"process":"producer","type":"ok","f":"publish","value":1,"stream_seq":1}`,
			`{"index":1,"process":"consumer","type":"ok","f":"deliver","value":1,"partition":1,"offset":5}`,
		), "line 3: a history numbers messages"},
		{"negative offset", lines(meta, `{"index":0,"process":"consumer","type":"ok","f":"deliver","value":1,"partition":0,"offset":-1}`), "line 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.in))
			if err == nil {
				t.Fatalf("Parse succeeded, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}
