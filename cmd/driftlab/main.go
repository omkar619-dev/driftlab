// Command driftlab judges a history file against the guarantees in
// DESIGN.md.
//
// Usage:
//
//	driftlab check <history.jsonl>
//
// The exit code is the verdict, so scripts can act on it: 0 clean, 1 failed,
// 2 invalid, and 3 when the command couldn't run at all (bad usage, or a
// file it can't open or parse).
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/omkar619-dev/driftlab/internal/checker"
	"github.com/omkar619-dev/driftlab/internal/history"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run does all the work and returns the exit code, because os.Exit skips
// deferred calls: main may only call it after run has returned.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "check" {
		fmt.Fprintln(stderr, "usage: driftlab check <history.jsonl>")
		return 3
	}
	f, err := os.Open(args[1])
	if err != nil {
		fmt.Fprintln(stderr, "driftlab:", err)
		return 3
	}
	defer f.Close()
	h, err := history.Parse(f)
	if err != nil {
		fmt.Fprintf(stderr, "driftlab: %s: %v\n", args[1], err)
		return 3
	}
	r := checker.Check(h)
	for _, line := range r.Lines() {
		fmt.Fprintln(stdout, line)
	}
	switch r.Verdict {
	case checker.Clean:
		return 0
	case checker.Failed:
		return 1
	default:
		return 2
	}
}
