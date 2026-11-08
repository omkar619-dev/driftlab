// The protocol between the runner and a driver: the five commands the runner
// writes on the driver's stdin, and the events the driver writes on its
// stdout, one JSON object per line (DESIGN.md, Driver split).

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// protocol is the number setup carries. A new field that changes what a
// driver must do comes with a new number, and a driver refuses a number it
// doesn't know.
const protocol = 1

// command is one of the runner's five commands. Each kind is a struct of its
// own, so a line carries exactly its command's fields, in the order
// DESIGN.md's example shows them.
type command interface {
	name() string
}

// setupCmd asks the driver to make its producer, with the named set of
// settings and any options the scenario sets, and to answer ready.
type setupCmd struct {
	Cmd        string            `json:"cmd"`
	Protocol   int               `json:"protocol"`
	Brokers    []string          `json:"brokers"`
	Topic      string            `json:"topic"`
	Partitions int               `json:"partitions"`
	Settings   string            `json:"settings"`
	Options    map[string]string `json:"options"`
}

// publishCmd hands one value to the driver's producer, for one partition.
type publishCmd struct {
	Cmd       string `json:"cmd"`
	Value     uint64 `json:"value"`
	Partition int32  `json:"partition"`
}

// bareCmd is a command with no fields: subscribe, close-writer or
// stop-reader.
type bareCmd struct {
	Cmd string `json:"cmd"`
}

func (c setupCmd) name() string   { return c.Cmd }
func (c publishCmd) name() string { return c.Cmd }
func (c bareCmd) name() string    { return c.Cmd }

// newSetup makes a setup command. A nil options map is written as {}, not
// null, so a driver always finds an object there.
func newSetup(brokers []string, topic string, partitions int, settings string, options map[string]string) setupCmd {
	if options == nil {
		options = map[string]string{}
	}
	return setupCmd{
		Cmd:        "setup",
		Protocol:   protocol,
		Brokers:    brokers,
		Topic:      topic,
		Partitions: partitions,
		Settings:   settings,
		Options:    options,
	}
}

// newPublish makes a publish command.
func newPublish(value uint64, partition int32) publishCmd {
	return publishCmd{Cmd: "publish", Value: value, Partition: partition}
}

// The three commands with no fields.
var (
	subscribeCmd   = bareCmd{Cmd: "subscribe"}
	closeWriterCmd = bareCmd{Cmd: "close-writer"}
	stopReaderCmd  = bareCmd{Cmd: "stop-reader"}
)

// encodeCommand turns a command into its line, newline included.
func encodeCommand(c command) ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encoding %s: %w", c.name(), err)
	}
	return append(b, '\n'), nil
}

// event is one line a driver writes. Which fields it must carry, and which
// it may, depends on the event, as eventFields says; a field the event
// doesn't take stays at its zero value. Code is a pointer because it is
// optional: a client doesn't always give an error number.
type event struct {
	Event     string            `json:"event"`
	Driver    string            `json:"driver"`
	Client    string            `json:"client"`
	Settings  map[string]string `json:"settings"`
	Value     uint64            `json:"value"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Poll      uint64            `json:"poll"`
	Process   string            `json:"process"`
	Error     string            `json:"error"`
	Code      *int              `json:"code"`
}

// eventFields lists, for every event a driver may write, the fields it must
// carry and the ones it may. Any other field is wrong, and so is a time or a
// line number: only the runner writes those.
var eventFields = map[string]struct{ need, may []string }{
	"ready":          {need: []string{"driver", "client", "settings"}},
	"subscribed":     {},
	"taken":          {need: []string{"value"}},
	"ok":             {need: []string{"value", "partition", "offset"}},
	"info":           {need: []string{"value", "error"}, may: []string{"code"}},
	"fail":           {need: []string{"value", "error"}, may: []string{"code"}},
	"deliver":        {need: []string{"value", "partition", "offset", "poll"}},
	"error":          {need: []string{"process", "error"}},
	"writer-closed":  {},
	"reader-stopped": {},
}

// parseEvent reads one line from a driver and checks it: one JSON object, a
// known event, every field that event needs and no field it doesn't take,
// with values of the right type and range. Field names must match exactly,
// although encoding/json alone would also take "Value" for value. An error
// says what is wrong with the line, and the runner then treats the driver as
// broken (DESIGN.md, When a driver breaks).
func parseEvent(line []byte) (event, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return event{}, fmt.Errorf("not a JSON object: %w", err)
	}
	var name string
	if err := json.Unmarshal(fields["event"], &name); err != nil || name == "" {
		return event{}, errors.New(`no "event" name`)
	}
	spec, ok := eventFields[name]
	if !ok {
		return event{}, fmt.Errorf("unknown event %q", name)
	}
	for _, f := range spec.need {
		if v, ok := fields[f]; !ok || bytes.Equal(v, []byte("null")) {
			return event{}, fmt.Errorf("%s needs %q", name, f)
		}
	}
	for _, f := range slices.Sorted(maps.Keys(fields)) {
		if f != "event" && !slices.Contains(spec.need, f) && !slices.Contains(spec.may, f) {
			return event{}, fmt.Errorf("%s doesn't take %q", name, f)
		}
	}
	var e event
	if err := json.Unmarshal(line, &e); err != nil {
		return event{}, fmt.Errorf("%s: a field has the wrong type or range: %w", name, err)
	}
	if err := e.check(); err != nil {
		return event{}, fmt.Errorf("%s: %w", name, err)
	}
	return e, nil
}

// check holds an event's values to what they mean. Values start at 1, as in
// every driver. Positions start at 0, so a negative offset, such as
// librdkafka's -1001 for "unknown", is not one. Polls are counted from 1.
func (e event) check() error {
	switch {
	case e.Event == "ready" && (e.Driver == "" || e.Client == ""):
		return errors.New("needs a driver name and a client version")
	case slices.Contains(eventFields[e.Event].need, "value") && e.Value == 0:
		return errors.New("values start at 1")
	case e.Partition < 0 || e.Offset < 0:
		return fmt.Errorf("partition %d offset %d is not a position", e.Partition, e.Offset)
	case e.Event == "deliver" && e.Poll == 0:
		return errors.New("polls are counted from 1")
	case e.Event == "error" && e.Process != "producer" && e.Process != "consumer":
		return fmt.Errorf("process %q, want producer or consumer", e.Process)
	}
	return nil
}
