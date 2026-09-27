// Package history parses driftlab histories: a meta line followed by one
// operation per line, in the format DESIGN.md specifies.
package history

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Type is an operation's outcome, in Jepsen's vocabulary.
type Type string

const (
	Invoke Type = "invoke"
	OK     Type = "ok"
	Fail   Type = "fail"
	Info   Type = "info"
)

// Operations the checker understands. Drivers also record others, such as
// nemesis events and client errors, which the checker keeps as evidence.
const (
	Publish = "publish"
	Deliver = "deliver"
	Read    = "read"
)

// Nemesis is the process that records faults. Each fault is an operation:
// an invoke when it starts, and an ok once it has fully happened.
const Nemesis = "nemesis"

const metaType = "meta"

// Meta is a history's first line: what produced the run. Faults lists the
// faults the scenario set out to inject, whether or not they happened.
type Meta struct {
	Driver   string          `json:"driver"`
	Client   string          `json:"client"`
	API      string          `json:"api"`
	Server   string          `json:"server"`
	Scenario string          `json:"scenario"`
	Faults   []string        `json:"faults"`
	Params   json.RawMessage `json:"params"`
	Driftlab string          `json:"driftlab"`
}

// Op is one operation. Drivers number values from 1 and JetStream numbers
// stream sequences from 1, so 0 means "absent" in Value and StreamSeq.
type Op struct {
	Index     int             `json:"index"`
	Time      time.Duration   `json:"time_ns"`
	Process   string          `json:"process"`
	Type      Type            `json:"type"`
	F         string          `json:"f"`
	Value     uint64          `json:"value"`
	StreamSeq uint64          `json:"stream_seq"`
	Error     string          `json:"error"`
	Detail    json.RawMessage `json:"detail"`
}

// History is a parsed history file.
type History struct {
	Meta Meta
	Ops  []Op
}

// Parse reads and validates a history. It rejects malformed evidence
// instead of guessing, and each error names the line it came from.
func Parse(r io.Reader) (*History, error) {
	sc := bufio.NewScanner(r)
	h := &History{}
	sawMeta := false
	line := 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		if !sawMeta {
			m, err := parseMeta(b)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", line, err)
			}
			h.Meta = m
			sawMeta = true
			continue
		}
		var op Op
		if err := json.Unmarshal(b, &op); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if err := validate(op, len(h.Ops)); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		h.Ops = append(h.Ops, op)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("line %d: %w", line+1, err)
	}
	if !sawMeta {
		return nil, errors.New("history is empty: missing the meta line")
	}
	return h, nil
}

func parseMeta(b []byte) (Meta, error) {
	var first struct {
		Type string `json:"type"`
		Meta
	}
	if err := json.Unmarshal(b, &first); err != nil {
		return Meta{}, err
	}
	if first.Type != metaType {
		return Meta{}, fmt.Errorf("first record has type %q, want %q", first.Type, metaType)
	}
	if first.Client == "" || first.Scenario == "" {
		return Meta{}, errors.New("the meta line needs client and scenario")
	}
	return first.Meta, nil
}

func validate(op Op, wantIndex int) error {
	if op.Index != wantIndex {
		return fmt.Errorf("index is %d, want %d", op.Index, wantIndex)
	}
	switch op.Type {
	case Invoke, OK, Fail, Info:
	default:
		return fmt.Errorf("unknown type %q", op.Type)
	}
	if op.Process == "" || op.F == "" {
		return errors.New("process and f are required")
	}
	switch op.F {
	case Publish:
		if op.Value == 0 {
			return errors.New("publish needs a value")
		}
		if op.Type == OK && op.StreamSeq == 0 {
			return errors.New("an ok publish needs the stream_seq from its ack")
		}
	case Deliver, Read:
		if op.Type != OK {
			return fmt.Errorf("%s must have type ok", op.F)
		}
		if op.Value == 0 || op.StreamSeq == 0 {
			return fmt.Errorf("%s needs a value and a stream_seq", op.F)
		}
	}
	return nil
}
