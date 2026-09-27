package checker

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/omkar619-dev/driftlab/internal/history"
)

func summarize(as []Anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, fmt.Sprintf("%s %s ops=%v: %s", a.Kind, a.Process, a.Ops, a.Msg))
	}
	return out
}

func TestCheckFixtures(t *testing.T) {
	tests := []struct {
		file string
		want []string
	}{
		{"clean.jsonl", nil},
		{"poll-skip.jsonl", []string{
			"poll-skip consumer ops=[11 12]: expected stream_seq 3, got 5: 2 skipped, 0 delivered late, 2 never delivered",
		}},
		{"nonmonotonic-poll.jsonl", []string{
			"poll-skip consumer ops=[8 9]: expected stream_seq 2, got 3: 1 skipped, 1 delivered late, 0 never delivered",
			"nonmonotonic-poll consumer ops=[9 10]: stream_seq 2 delivered after 3",
		}},
		{"duplicate.jsonl", []string{
			"duplicate consumer ops=[9 10]: stream_seq 2 delivered again",
		}},
		{"first-skip.jsonl", []string{
			"poll-skip consumer ops=[6]: expected stream_seq 1, got 2: 1 skipped, 0 delivered late, 1 never delivered",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			h, err := history.Parse(f)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := summarize(Check(h))
			if !slices.Equal(got, tt.want) {
				t.Errorf("anomalies:\n  got  %q\n  want %q", got, tt.want)
			}
		})
	}
}
