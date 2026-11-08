package main

import (
	"strings"
	"testing"
)

// The command lines are exactly the ones DESIGN.md shows, since drivers in
// other languages are written from that example.
func TestEncodeCommand(t *testing.T) {
	brokers := []string{"127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092"}
	for _, tc := range []struct {
		cmd  command
		want string
	}{
		{newSetup(brokers, "driftlab-follower-cut-1762600000000", 1, "defaults", nil),
			`{"cmd":"setup","protocol":1,"brokers":["127.0.0.1:29092","127.0.0.1:39092","127.0.0.1:49092"],"topic":"driftlab-follower-cut-1762600000000","partitions":1,"settings":"defaults","options":{}}`},
		{newSetup(brokers[:1], "t", 3, "idempotent", map[string]string{"produce_timeout": "5s"}),
			`{"cmd":"setup","protocol":1,"brokers":["127.0.0.1:29092"],"topic":"t","partitions":3,"settings":"idempotent","options":{"produce_timeout":"5s"}}`},
		{subscribeCmd, `{"cmd":"subscribe"}`},
		{newPublish(1, 0), `{"cmd":"publish","value":1,"partition":0}`},
		{closeWriterCmd, `{"cmd":"close-writer"}`},
		{stopReaderCmd, `{"cmd":"stop-reader"}`},
	} {
		got, err := encodeCommand(tc.cmd)
		if err != nil {
			t.Fatalf("%s: %v", tc.cmd.name(), err)
		}
		if string(got) != tc.want+"\n" {
			t.Errorf("%s:\ngot  %s\nwant %s", tc.cmd.name(), got, tc.want)
		}
	}
}

// Every event line in DESIGN.md's example parses, and so does one of each
// other event.
func TestParseEventGood(t *testing.T) {
	for _, line := range []string{
		`{"event":"ready","driver":"java","client":"org.apache.kafka:kafka-clients@4.4.0","settings":{"acks":"all","enable.idempotence":"true"}}`,
		`{"event":"subscribed"}`,
		`{"event":"taken","value":1}`,
		`{"event":"ok","value":1,"partition":0,"offset":0}`,
		`{"event":"deliver","value":1,"partition":0,"offset":0,"poll":1}`,
		`{"event":"info","value":2,"error":"The request timed out.","code":7}`,
		`{"event":"info","value":3,"error":"delivery timed out"}`,
		`{"event":"info","value":4,"error":"no number given","code":null}`,
		`{"event":"fail","value":5,"error":"not written","code":10}`,
		`{"event":"error","process":"consumer","error":"connection reset"}`,
		`{"event":"writer-closed"}`,
		`{"event":"reader-stopped"}`,
	} {
		if _, err := parseEvent([]byte(line)); err != nil {
			t.Errorf("%s: %v", line, err)
		}
	}
}

// A parsed event carries the line's values, and Code is nil when the line
// has no code.
func TestParseEventValues(t *testing.T) {
	e, err := parseEvent([]byte(`{"event":"ok","value":17,"partition":2,"offset":40}`))
	if err != nil || e.Event != "ok" || e.Value != 17 || e.Partition != 2 || e.Offset != 40 || e.Code != nil {
		t.Errorf("ok: got %+v, %v", e, err)
	}
	e, err = parseEvent([]byte(`{"event":"info","value":2,"error":"timed out","code":7}`))
	if err != nil || e.Error != "timed out" || e.Code == nil || *e.Code != 7 {
		t.Errorf("info: got %+v, %v", e, err)
	}
	e, err = parseEvent([]byte(`{"event":"ready","driver":"java","client":"c@1","settings":{"acks":"all"}}`))
	if err != nil || e.Driver != "java" || e.Client != "c@1" || e.Settings["acks"] != "all" {
		t.Errorf("ready: got %+v, %v", e, err)
	}
}

// A line that isn't one of the events, or breaks one of their rules, is
// refused with an error that says why.
func TestParseEventBad(t *testing.T) {
	for _, tc := range []struct{ line, want string }{
		{``, "not a JSON object"},
		{`taken 17`, "not a JSON object"},
		{`["taken",17]`, "not a JSON object"},
		{`{"event":"taken","value":1} {"event":"taken","value":2}`, "not a JSON object"},
		{`{"value":1}`, `no "event" name`},
		{`{"event":7}`, `no "event" name`},
		{`{"event":"acked","value":1}`, `unknown event "acked"`},
		{`{"event":"ok","value":1,"partition":0}`, `ok needs "offset"`},
		{`{"event":"ok","value":1,"partition":0,"offset":null}`, `ok needs "offset"`},
		{`{"event":"ok","Value":1,"partition":0,"offset":0}`, `ok needs "value"`},
		{`{"event":"ok","value":1,"partition":0,"offset":0,"time_ns":5}`, `ok doesn't take "time_ns"`},
		{`{"event":"subscribed","index":0}`, `subscribed doesn't take "index"`},
		{`{"event":"taken","value":"17"}`, "wrong type or range"},
		{`{"event":"taken","value":1.5}`, "wrong type or range"},
		{`{"event":"taken","value":-1}`, "wrong type or range"},
		{`{"event":"ok","value":1,"partition":4294967296,"offset":0}`, "wrong type or range"},
		{`{"event":"ready","driver":"python","client":"kafka-python@3.0.13","settings":{"acks":1}}`, "wrong type or range"},
		{`{"event":"taken","value":0}`, "values start at 1"},
		{`{"event":"ok","value":1,"partition":0,"offset":-1001}`, "not a position"},
		{`{"event":"deliver","value":1,"partition":0,"offset":0,"poll":0}`, "polls are counted from 1"},
		{`{"event":"error","process":"nemesis","error":"x"}`, `process "nemesis"`},
		{`{"event":"ready","driver":"java","client":"","settings":{}}`, "needs a driver name and a client version"},
	} {
		_, err := parseEvent([]byte(tc.line))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got error %v, want one containing %q", tc.line, err, tc.want)
		}
	}
}
