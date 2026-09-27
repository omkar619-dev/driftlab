package checker

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/omkar619-dev/driftlab/internal/history"
)

func TestCheckFixtures(t *testing.T) {
	tests := []struct {
		file string
		want []string
	}{
		{"clean.jsonl", []string{
			"verdict clean, 0 of 4 publishes unknown",
		}},
		{"poll-skip.jsonl", []string{
			"verdict failed, 0 of 5 publishes unknown",
			"poll-skip consumer ops=[11 12]: expected stream_seq 3, got 5: 2 skipped, 0 delivered late, 2 never delivered",
		}},
		{"nonmonotonic-poll.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"poll-skip consumer ops=[8 9]: expected stream_seq 2, got 3: 1 skipped, 1 delivered late, 0 never delivered",
			"nonmonotonic-poll consumer ops=[9 10]: stream_seq 2 delivered after 3",
		}},
		{"duplicate.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"duplicate consumer ops=[9 10]: stream_seq 2 delivered again",
		}},
		{"first-skip.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"poll-skip consumer ops=[6]: expected stream_seq 1, got 2: 1 skipped, 0 delivered late, 1 never delivered",
		}},
		{"no-final-read.jsonl", []string{
			"verdict invalid, 0 of 2 publishes unknown",
			"invalid: no final read, so the stream's contents are unknown",
		}},
		{"gapped-stream.jsonl", []string{
			"verdict invalid, 1 of 4 publishes unknown",
			"invalid: final read has a hole no acked publish explains: 1 of 4 stream sequences missing, starting at 3",
			"poll-skip consumer ops=[9 10]: expected stream_seq 3, got 4: 1 skipped, 0 delivered late, 1 never delivered",
		}},
		{"fault-no-evidence.jsonl", []string{
			"verdict invalid, 0 of 2 publishes unknown",
			`invalid: declared fault "stall" never completed`,
		}},
		{"fault-with-evidence.jsonl", []string{
			"verdict clean, 1 of 4 publishes unknown",
		}},
		{"lost-write.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"lost-write producer ops=[5]: value 3 was acked at stream_seq 3 but is missing from the final read",
		}},
		{"acked-hole.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"lost-write producer ops=[5]: value 3 was acked at stream_seq 3 but is missing from the final read",
		}},
		{"phantom.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"phantom consumer ops=[7 11]: value 2 appeared although its publish failed",
			"phantom consumer ops=[9 13]: value 7 was never published",
		}},
		{"stall.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"stall consumer ops=[10 14]: had reached stream_seq 3 of 4 when the final read began",
		}},
		{"silent-consumer.jsonl", []string{
			"verdict failed, 0 of 2 publishes unknown",
			"stall consumer ops=[5 7]: had received nothing when the final read began; the stream ends at stream_seq 2",
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
			got := Check(h).Lines()
			if !slices.Equal(got, tt.want) {
				t.Errorf("result:\n  got  %q\n  want %q", got, tt.want)
			}
		})
	}
}
