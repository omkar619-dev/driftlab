package main

import (
	"io"
	"testing"
)

func TestRunExitCodes(t *testing.T) {
	fixtures := "../../internal/checker/testdata/"
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"clean run", []string{"check", fixtures + "clean.jsonl"}, 0},
		{"failed run", []string{"check", fixtures + "poll-skip.jsonl"}, 1},
		{"invalid run", []string{"check", fixtures + "gapped-stream.jsonl"}, 2},
		{"malformed history", []string{"check", "testdata/malformed.jsonl"}, 3},
		{"missing file", []string{"check", "testdata/does-not-exist.jsonl"}, 3},
		{"no file given", []string{"check"}, 3},
		{"unknown command", []string{"judge", fixtures + "clean.jsonl"}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := run(tt.args, io.Discard, io.Discard); got != tt.want {
				t.Errorf("run(%q) = %d, want %d", tt.args, got, tt.want)
			}
		})
	}
}
