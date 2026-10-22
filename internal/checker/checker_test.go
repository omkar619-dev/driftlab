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
		{"duplicate-delivery.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"duplicate-delivery consumer ops=[9 10]: stream_seq 2 delivered again",
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
			"recovery consumer: caught up 0s after the faults ended",
			"lag consumer: the longest wait from an ack to its delivery was 0s (value 1)",
		}},
		{"fault-then-skip.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"poll-skip consumer ops=[6 9]: expected stream_seq 2, got 3: 1 skipped, 0 delivered late, 1 never delivered",
			"recovery consumer: never caught up after the faults ended",
			"lag consumer: the longest wait from an ack to its delivery was 0s (value 1)",
		}},
		{"deaf-after-fault.jsonl", []string{
			"verdict clean, 1 of 4 publishes unknown",
			"recovery consumer: caught up 0s after the faults ended",
			"lag consumer: the longest wait from an ack to its delivery was 13.801s (value 3)",
		}},
		{"lost-write.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"lost-write producer ops=[5]: value 3 was acked at stream_seq 3 but is missing from the final read",
		}},
		{"acked-hole.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"lost-write producer ops=[5]: value 3 was acked at stream_seq 3 but is missing from the final read",
		}},
		{"start-new-clean.jsonl", []string{
			"verdict clean, 0 of 5 publishes unknown",
		}},
		{"start-new-fault.jsonl", []string{
			"verdict clean, 0 of 5 publishes unknown",
			"lag consumer: the longest wait from an ack to its delivery was 700ms (value 5)",
		}},
		{"sub-death.jsonl", []string{
			"verdict failed, 0 of 5 publishes unknown",
			"sub-death consumer-a ops=[12 16]: stopped at stream_seq 2 and received nothing more, while consumer-b went on to stream_seq 5",
		}},
		{"both-slow.jsonl", []string{
			"verdict failed, 0 of 5 publishes unknown",
			"stall consumer-a ops=[12 18]: had reached stream_seq 2 of 5 when the final read began",
			"stall consumer-b ops=[13 18]: had reached stream_seq 2 of 5 when the final read began",
		}},
		{"before-start.jsonl", []string{
			"verdict failed, 0 of 5 publishes unknown",
			"before-start consumer ops=[10 11 12]: delivered stream_seq 1 to 3 after asking to start at stream_seq 4",
		}},
		{"before-start-fault.jsonl", []string{
			"verdict failed, 0 of 5 publishes unknown",
			"before-start consumer ops=[12 13 14]: delivered stream_seq 1 to 3 after asking to start at stream_seq 4",
			"lag consumer: the longest wait from an ack to its delivery was 700ms (value 5)",
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
		{"kafka-clean.jsonl", []string{
			"verdict clean, 0 of 4 publishes unknown",
		}},
		{"kafka-first-skip.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"poll-skip consumer ops=[12]: expected partition 1 offset 0, got 1: 1 skipped, 0 delivered late, 1 never delivered",
		}},
		{"kafka-hole.jsonl", []string{
			"verdict invalid, 1 of 4 publishes unknown",
			"invalid: final read has a hole no acked publish explains: 1 of 2 offsets in partition 1 missing, starting at 0",
		}},
		{"kafka-stall.jsonl", []string{
			"verdict failed, 0 of 2 publishes unknown",
			"stall consumer ops=[6 8]: had received nothing from partition 1 when the final read began; it ends at offset 0",
		}},
		{"kafka-recovery.jsonl", []string{
			"verdict clean, 0 of 3 publishes unknown",
			"recovery consumer: caught up 2.5s after the faults ended",
			"lag consumer: the longest wait from an ack to its delivery was 11.497s (value 2)",
		}},
		{"kafka-1217.jsonl", []string{
			"verdict failed, 1 of 3 publishes unknown",
			"lost-write producer ops=[5]: value 3 was acked at partition 0 offset 1 but is missing from the final read",
			"inconsistent-offsets producer ops=[5 7]: value 3 was acked at partition 0 offset 1, but the final read has value 2 there",
		}},
		{"kafka-mislabeled.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"inconsistent-offsets consumer ops=[7 10]: value 3 was delivered from partition 0 offset 1, but the final read has value 2 there",
		}},
		{"kafka-duplicate.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"duplicate final-read ops=[7 8]: value 2 is stored at 2 positions: partition 0 offset 1, partition 0 offset 2",
		}},
		{"kafka-reordered.jsonl", []string{
			"verdict failed, 0 of 3 publishes unknown",
			"nonmonotonic-send producer ops=[4 5]: value 3 was sent after value 2 but acked at partition 0 offset 1, not after value 2 at partition 0 offset 2",
		}},
		{"relay-clean.jsonl", []string{
			"verdict clean, 0 of 4 publishes unknown",
		}},
		{"relay-collapsed.jsonl", []string{
			"verdict clean, 0 of 4 publishes unknown",
		}},
		{"relay-restart.jsonl", []string{
			"verdict failed, 0 of 8 publishes unknown",
			"lost-write producer ops=[9]: value 5 was acked by the source at partition 0 offset 4 but is missing from the copy",
			"lost-write producer ops=[11]: value 6 was acked by the source at partition 0 offset 5 but is missing from the copy",
			"duplicate final-read ops=[17 19]: value 2 is stored at 2 positions: partition 0 offset 1, partition 0 offset 3",
			"duplicate final-read ops=[18 20]: value 3 is stored at 2 positions: partition 0 offset 2, partition 0 offset 4",
		}},
		{"relay-reordered.jsonl", []string{
			"verdict failed, 0 of 4 publishes unknown",
			"nonmonotonic-copy final-read ops=[9 10]: value 2 sits after value 3 in the copy (partition 0 offset 2, after partition 0 offset 1), but before it in the source (partition 0 offset 1, before partition 0 offset 2)",
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
