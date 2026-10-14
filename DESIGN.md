# driftlab: design

> Status: last updated 2026-10-13. v0 is done: the checker, and one system (NATS JetStream, Go
> client) under stall, network and server faults. The calibration matrix reproduces, and weekend 3
> produced two upstream reports (see Weekend 3 results). v1, the Go Kafka clients, has started: the
> three-broker cluster runs, and v1 design records the decisions its code will follow.

## What driftlab is

driftlab records what a streaming client library actually did under faults, then checks that
record against the delivery guarantees the library documents.

## Why it exists, and what it is not

Jepsen asks whether a *server* keeps its promises. Its NATS 2.12.1 analysis
(https://jepsen.io/analyses/nats-2.12.1) tested JetStream durability through the Java client, and
it explicitly left consumer-side delivery unchecked: whether a single consumer misses messages, or
receives them out of order.

That gap is where real bugs live. https://github.com/nats-io/nats.go/issues/2107 was the Go
client's ordered consumer silently losing messages while the server behaved correctly.

driftlab's lane is **the client library you actually import**. It runs the same scenario against
different client APIs and versions, and one checker judges all of them.

What is *not* new, stated up front: the anomaly vocabulary (lost-write, poll-skip,
nonmonotonic-poll, nonmonotonic-send, duplicate, inconsistent-offsets) comes from Jepsen's Kafka
workload (https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html). We reuse those names for
the same failures on purpose, so anyone who knows Jepsen can read driftlab output without a
glossary. The other names (stall, phantom, duplicate-delivery) are driftlab's own. `lost-write`
is judged differently. Jepsen infers it from reads: an acked message that nobody read, although
someone read a later offset; an acked message nobody read at all is `unseen`. driftlab checks each
acked message against a final read of the whole log instead, so it can tell a message the log
lost (a lost write) from one the log kept but a consumer never got (a stall or a poll-skip), and
it also catches a message that a consumer received before the log lost it.

## First principles

Every decision below follows from one of these.

1. **A guarantee is a sentence in the docs.** "Reliable" can't be tested. "An ordered consumer
   delivers every message in stream order, with no gaps, and recovers on its own" can. Every
   scenario starts by quoting the contract it checks.
2. **Record first, judge later.** A run writes a *history*: a JSONL log of everything that
   happened. A separate, pure checker reads it afterwards. Runs are nondeterministic and expensive
   to repeat, while histories are cheap to keep. When the checker has a bug, fix it and re-check
   the old histories without re-running anything. This is the same shape as the MIT 6.5840 KV
   tests, which record every Get/Put and hand that history to porcupine.
3. **An operation has three outcomes, not two.** They are `ok` (it definitely happened), `fail`
   (it definitely did not) and `info` (unknown). A publish that timed out may still be in the
   stream, and may even land later. If we recorded it as `fail` and it did land, the checker would
   report a phantom: a message that supposedly never happened, sitting in the stream. The price of
   `info` is that we can only accuse the server of losing messages we have a receipt (an ack) for.
   If the server accepted a message, the ack got lost on the way back and the server then lost the
   message, the history looks exactly like a publish that never arrived.
4. **Order comes from the system's sequence numbers, never from clocks.** JetStream stamps each
   stored message with a stream sequence, and that number alone decides order and gaps.
   Timestamps are for humans reading the timeline; no verdict depends on them. Even liveness is
   judged by position: the driver waits a grace window after the last fault before it starts the
   final read, and a consumer that hasn't caught up by then has stalled.
5. **Remove every legitimate reason for a gap.** Use one subject, no retention limits and no
   deletes. Stream sequences are then exactly 1..N, and the ordered-consumer contract collapses to
   one line: *the delivered stream sequences must be 1, 2, …, N.* Any deviation is a bug, and the
   checker only has to classify it. The driver enforces this: every run gets a fresh stream that
   denies deletes and purges, so the server itself refuses to make a hole.
6. **Calibrate before you measure.** A checker that has never reported a violation is untested.
   Before any "finding" means anything, the checker must (a) flag hand-written bad histories,
   (b) catch a real, known bug, and (c) pass the fix for that bug.
7. **Every experiment has a control.** Each scenario also runs with no fault injected. An anomaly
   in the control run means the harness or the environment is broken, not the system under test.
8. **The injected fault should be the only uncontrolled thing.** The harness and the broker run
   on the same wired machine. A flaky NIC, a Wi-Fi hop or a host that's paging produces
   "violations" nobody can attribute to anything.

## v0 scope

**Server:** `nats:2.14.7` (single node, file storage). This is the same minor version the 2107
reporter used, at its latest patch.

**Client:** `github.com/nats-io/nats.go`, with two consumer APIs from the same library:

- `legacy`: the push ordered consumer, `js.Subscribe(subj, cb, nats.OrderedConsumer())`. KV and
  Object Store watchers are built on this path.
- `jetstream`: the ordered consumer from the newer `jetstream` package.

**Client versions (the calibration pair):**

- `v1.53.1` is the last release that has the 2107 bug.
- `v1.54.0` contains the fix, https://github.com/nats-io/nats.go/pull/2137 (merged 2026-09-18).

**Faults.** The calibration fault is client-side: the consumer's callback stalls while a low pending
limit is set, which is exactly the 2107 trigger. Weekend 3 added network faults, through toxiproxy,
and server faults, through docker.

**Non-goals for v0:** multi-node clusters, Kafka/Redpanda/Redis, exactly-once, durable acked
consumers and redelivery, and a second machine.

## Scenarios

Each scenario follows the reproduction in the issue, with one change. It publishes one message,
starts the ordered consumer and waits for that message to arrive, which is where a stall
scenario's first callback blocks. Only then does it shrink the pending tray to 20 messages and
publish the other 99, so the flood always arrives after the trap is armed. It then releases the
stall, publishes one tail message, gives the consumer up to a grace window (10 seconds by default)
to catch up, stops it, and does the final read.

The change is a finding from the first runs. The issue publishes the whole backlog before the
consumer starts, and so did driftlab at first. But shrinking the tray only turns away new
arrivals, so whether it overflows depends on how much of the backlog has already landed, and that
is a race. The fixed client's first stall runs came back invalid: its tray never overflowed,
because the backlog had most likely already landed in it. Control and stall scenarios now publish
in the same order, so they differ only by the fault.

| Scenario | Stall | Why it exists |
|---|---|---|
| `publish-only` | none, and no consumer | proves the driver and the final read on their own |
| `control` | none | proves the harness and environment are clean |
| `proxy-control` | none, but the consumer goes through toxiproxy | the control for weekend 3's network faults: the proxy on its own must change nothing |
| `slow-short` | 2s, shorter than the 5s ordered-consumer heartbeat | the path in the issue: no reset ever fires |
| `slow-long` | 8s, longer than the heartbeat | the heartbeat reset fires, but it resumes from a sequence that has already been advanced past messages that were never delivered |
| `net-slow` | none; the consumer's line runs 25s late for 30s, from halfway through the flood | longer than the 20s the legacy consumer can take to decide its line is dead, so it resets while old messages are still in flight; the `jetstream` one waits 30s, so it has to ride the delay out without resetting |
| `net-cut` | none; the consumer's line is cut for 8s, from halfway through the flood | the connection drops with messages in flight, and the consumer has to resume from exactly the right place after reconnecting |
| `server-pause` | none; nats-server is frozen for 35s, from halfway through the flood | every connection stays open but nothing answers, for longer than either consumer waits before resetting |
| `server-kill` | none; nats-server is killed halfway through the flood and started again after 5s | both connections drop, and the restart deletes both consumers' server-side state, which they have to rebuild |

`slow-long` matters because a fix that only covers the short path passes `slow-short` and still
fails here.

A stall scenario declares two faults. `stall` is the blocked callback. `overflow` is what the stall
is meant to cause: the client's pending tray fills up and nats.go drops messages, which it reports
as a slow consumer error. The driver marks `overflow` as done only when that error arrives, so a
run whose tray never overflowed is invalid rather than clean, because the 2107 trigger never fired.
The `jetstream` API has no drop path. It pulls messages in batches of at most `PullMaxMessages`
(its tray, set to the same size) and asks for more only when there is room, so its stall
scenarios declare only `stall`. The driver picks the API with `-api legacy` or `-api jetstream`.

Weekend 3 breaks the consumer's connection on purpose, so the driver now uses two. The producer,
the stream setup and the final read share a direct connection that no network fault touches, so
publishes keep their acks. The consumer has a connection of its own. In a proxy scenario it reaches
nats-server through toxiproxy (`deploy/compose.yaml`): the driver creates the `helper-line` proxy
through toxiproxy's HTTP API at the start of the run, clears any fault an earlier run left behind,
and records `"via": "toxiproxy"` in the meta line.

The network scenarios break that line halfway through the flood, after the first half has been
published, and heal it after a hold. `net-cut` disables `helper-line` for 8 seconds: toxiproxy
drops the consumer's connection and refuses new ones, and nats.go retries every 2 seconds. The cut
only counts as done if the consumer's client really was disconnected when the line healed.
`net-slow` makes everything the server sends the consumer arrive 25 seconds late, for 30 seconds.
The legacy ordered consumer checks for activity every 10 seconds and resets after a whole interval
with no message or heartbeat, so a line that stays quiet for over 20 seconds always trips it, while
the old messages are still on their way. The `jetstream` one resets after two missed heartbeats,
and its heartbeat is 15 seconds: the package works out a 5-second default for ordered consumers
but never passes it on to the pull subscription, which falls back to half of the 30-second pull
expiry. Its pull requests show it, asking for `"idle_heartbeat":15000000000`. So it waits 30
seconds, rides this delay out without resetting, and takes the late messages as they arrive.

The server scenarios hit nats-server itself, halfway through the flood, so both connections lose
it at once. `server-pause` freezes its container for 35 seconds with `docker pause`: every
connection stays open, but nothing answers. That is longer than either consumer waits before
deciding its line is dead, up to 20 seconds for the legacy one and 30 for the `jetstream` one, so
both try to reset against a server that can't reply. `server-kill` kills the container with
`docker kill`, keeps it down for 5 seconds and starts it again. Both ordered consumers keep their
server-side consumer in memory only, so a restart deletes it however short the outage, and each
has to notice and build a new one. The producer keeps publishing through either fault, and a
publish whose ack doesn't arrive within the 5-second timeout is recorded as `info`, because it may
still land once the server is back (principle 3). The final read settles which ones did. The fault
starts at once and ends on its own timer, and it counts as done only when docker shows it took
hold: a paused container, or one that exited with code 137 (killed) and came back as a server with
a new ID, since nats-server picks a fresh one every time it starts. The driver runs docker itself,
so it has to run on the machine that hosts the container (`-container`, `driftlab-nats-1` by
default).

## Calibration matrix (the v0 exit criterion)

The expected verdicts come from the issue and the fix, and driftlab must reproduce them from
black-box observation alone. It does. These are the results of the first full run, on 2026-10-06
at commit `f1a2136`, with each consumer's recovery time after the stall ended:

| Scenario | legacy @ v1.53.1 | legacy @ v1.54.0 | jetstream @ v1.53.1 | jetstream @ v1.54.0 |
|---|---|---|---|---|
| control | clean | clean | clean | clean |
| slow-short | **failed**: poll-skip, 21–100 never delivered; never caught up | clean; caught up in 18 s | clean; 3 ms | clean; 3 ms |
| slow-long | **failed**: poll-skip, 21–100 never delivered; never caught up | clean; caught up in 10 s | clean; 3 ms | clean; 3 ms |

- Both failures match the issue exactly: the consumer delivers 1 to 20, jumps to 101, and the 80
  messages in between never arrive.
- `legacy @ v1.54.0` passes `slow-long` too, so the fix also covers the heartbeat path.
- The `jetstream` API is clean even on v1.53.1. It pulls only what fits in its tray, so it never
  drops a message, and 2107 needs a dropped message to happen.

`scripts/matrix.sh` reproduces the table, and also runs `proxy-control` and the four network and
server scenarios, which should be clean everywhere, so a full run is 32 runs. Run it on the machine
that hosts nats-server and toxiproxy: it builds the checker and both driver versions from the
current commit, runs every combination, prints each run's verdict, recovery and lag, and marks any
verdict the table doesn't predict. It passes `-grace 60s` instead of the default 10 seconds. The
fixed legacy consumer's recovery varies from run to run (between 8 and 18 seconds so far, with
identical settings), so under the default window its clean verdict came down to luck. A consumer
that catches up early ends the wait early, so only a stuck one waits out the full window.

## Weekend 3 results (network and server faults)

The full matrix ran on 2026-10-13 at commit `35fd84e`, and all 32 verdicts came out as the table
above predicts: only the two stalled runs of the legacy API on v1.53.1 failed. Every network and
server run was clean, so nothing was lost, duplicated or reordered, and race-detector builds of the
driver stayed silent through the same faults. What the faults exposed is liveness: how long a
consumer that loses nothing can stay silent. The lag of each fault scenario, with the two nats.go
versions within a few hundred milliseconds of each other:

| Scenario | legacy | jetstream |
|---|---|---|
| `net-slow` | 30.0 s | 25.0 s |
| `net-cut` | 20.0 s | 8.2–8.4 s |
| `server-pause` | 1 ms | 1 ms |
| `server-kill` | 13.8 s | 1–3 ms |

- **After a disconnect longer than about 5 seconds, the legacy consumer goes silent.** The server
  deletes its ephemeral consumer after the default 5-second inactive threshold (the stream's
  consumer count drops from 1 to 0), the client doesn't rebuild it when it reconnects, and it only
  notices at its next 10-second activity check. With the line cut right after subscribing,
  delivery came back 1.1 s after a 3 s cut, 0.03 s after 4 s, 14.0 s after 6 s and 12.0 s after
  8 s. A server restart causes the same silence however short it is, because the consumer lives in
  memory only (`server-kill`). KV watchers in both APIs are built on this consumer:
  `drivers/natsgo/repro2180`, a standalone reproducer that needs only nats-server, measured 10.00 s
  of silence after an 8 s cut for the push ordered consumer and for both KV watchers, against
  0.17 s for the `jetstream` ordered consumer. Reported as
  https://github.com/nats-io/nats.go/issues/2180.
- **The `jetstream` consumer's silence alarm takes 30 seconds, not 10.** The package works out a
  5-second heartbeat for ordered consumers but never passes it on (see the network paragraph
  above). So `net-slow`'s 25 seconds of silence never raised `ErrNoHeartbeat`, and
  `server-pause`'s 35 seconds raised it at 30.04 s. Reported as
  https://github.com/nats-io/nats.go/issues/2181.
- **`info` really does mean unknown.** All seven publishes that timed out during `server-pause`,
  and the one during `server-kill`, still landed in the stream once the server was back. Recording
  them as `fail` would have turned each one into a phantom (principle 3).
- **One stream create failed, once,** with `could not create storage directory` (`mkdir
  /data/jetstream/$G/streams: no such file or directory`), right after the previous run had
  deleted the stream. It didn't come back in 300 publish-only runs, so it is parked as seen once.

## v1 design: the Go Kafka clients

Decided on 2026-10-13, before any v1 code was written. The sections after this one describe what
the code does today, and change when the code does.

v1 runs the same experiment against Kafka's Go clients, starting with franz-go. The cluster is
three Kafka 4.3.1 nodes, each both a broker and a KRaft controller (`deploy/kafka.yaml`). Every
partition has a copy on all three, and an `acks=all` write needs two of them
(`min.insync.replicas=2`). Explored by hand, the cluster replaced a killed partition leader within
about 9 seconds (the brokers' session timeout), kept accepting `acks=all` writes with one broker
down, and didn't hand leadership back right away when the killed broker returned.

**Positions.** NATS gives a stored message one number, its stream sequence, counted from 1. Kafka
gives it two: a partition, and an offset within that partition, both counted from 0. So 0 can't
mean "absent" for them, the way it does for `value` and `stream_seq`. Kafka histories carry two
new fields, `partition` and `offset`, which the history package decodes into pointers, so a
missing field and a real 0 stay different: a driver that forgets one gets a parse error instead
of a fake first message. Writing offset+1 was rejected because the history would disagree with
Kafka's own tools, and so was -1 for "absent", because a Go field nobody filled in is 0 and would
read as a real position. NATS histories keep `stream_seq`, so every v0 history still parses and
gets the same verdict. The checker applies the v0 rules once per partition.

**Batches.** A Kafka consumer polls, and each poll returns a batch, possibly from several
partitions. Jepsen records a poll as one operation. driftlab keeps one `deliver` line per message,
because the checker's rules are about messages, and a search for one value still finds every
sighting of it. Each line carries the time its poll returned and a `poll` number, counted per
consumer, so the batch boundaries survive: Jepsen uses them to tell a skip inside one poll from a
skip between polls. The checker ignores `poll` until a finding needs it.

**Final read.** NATS lets the driver read a stream back message by message, without a consumer.
Kafka has no such call: every read is a fetch. So the final read uses a fresh client, separate from
the producer, with no consumer group and no committed offsets, that reads every partition from
offset 0 to its end. The hole rule still checks that reader: offsets must run 0, 1, 2 and so on in
every partition, and a hole no ack explains makes the run invalid. A hole an ack does explain is
reported as a lost write, and an acked message the reader skipped would look exactly the same. So
before a lost write goes upstream, a second witness reads the topic again: Kafka's own Java console
consumer, which shares no code with the Go client. Every run writes to a topic of its own, so the
evidence stays in the cluster until it is torn down. Kafka offsets have no legitimate holes only
while nothing uses transactions, whose markers take up offsets (principle 5). That changes in v2.

**Two new anomalies, and a rename.** Both new ones are Jepsen's names, with Jepsen's meanings:

| Anomaly | Definition | Usually whose bug |
|---|---|---|
| `duplicate` | one value stored at more than one position, which an idempotent producer exists to prevent | client (producer) or server |
| `inconsistent-offsets` | two observations disagree about which value sits at a position: an ack and a read, or two reads | client or server |

v0 used `duplicate` for a consumer receiving the same stream sequence twice, which Jepsen counts
as a `nonmonotonic-poll`. This document promises Jepsen's meanings, so v0's anomaly is now
`duplicate-delivery`. Old histories keep their verdicts; only the label changed.

**Calibration.** In https://github.com/twmb/franz-go/issues/1217, an idempotent produce timed out
and the client treated it as never written, letting the caller cancel it, although the broker had
appended it. The next batch reused its sequence numbers, so the broker took it for a retry and
acked it without writing it. The fix, https://github.com/twmb/franz-go/pull/1218, shipped in
v1.20.7: a batch the broker answered with REQUEST_TIMED_OUT is now marked unsure, and an unsure
batch can't be canceled. So v1.20.6 must fail and v1.20.7 must pass, the same shape as the 2107
pair.

**The 1217 scenario.** The reporter's reproducer is probabilistic: it loads the CPU and hopes for a
timeout. driftlab makes the timeout certain. In `followers-pause` the topic has one partition,
every send has a 2-second deadline, and halfway through the flood the driver freezes the two
brokers that hold the partition's other copies, with `docker pause`. With two of three brokers
frozen there is also no controller quorum, so nothing can drop them from the in-sync set: the
leader stores each record, can't get it copied, and answers REQUEST_TIMED_OUT once franz-go's
default 10-second produce timeout runs out. The freeze lasts 25 seconds. The run declares three
faults, each with evidence of its own: the freeze (docker shows both brokers paused), the timeout
(franz-go's debug log shows REQUEST_TIMED_OUT) and a send that came back after its deadline.
Without all three, the trigger didn't fire, and the run is invalid rather than clean. Because
franz-go's errors are recorded as `fail`, a v1.20.6 history should show a phantom, the canceled
record that the broker kept, plus a lost write and an inconsistent offset, a later record acked at
the canceled one's offset. `deadline-control` is the same run without the freeze and should be
clean on both versions. `scripts/kafka-calibrate.sh` runs both scenarios three times on each.

**Orchestration.** Still a script. v1 starts with v0's shape, one client library in two versions,
and the faults stay inside each driver. A Go `driftlab run` comes with v3, whose fault schedules
have to be generated, re-run and shrunk.

**The witness.** `drivers/franzgo` is a module of its own, built twice like the NATS driver: its
`go.mod` pins franz-go v1.20.7, and `v1.20.6.mod` pins the release before the fix. Both franz-go
releases declare Go 1.24 and need the same `kmsg` v1.12.0, so the two binaries differ only in
franz-go. The driver creates its topic and finds where each partition ends with raw `kmsg`
requests, not franz-go's admin package `kadm`: `kadm` requires franz-go v1.22.1, and importing it
would quietly upgrade the v1.20.6 build. Each run gets a fresh three-partition topic, and value v
goes to partition (v - 1) mod 3, so a history can be checked by hand. The producer keeps
franz-go's defaults, which are what users get: acks from every in-sync replica, and idempotent
writes. In `control`, a solo helper reads the topic while it is written: a client of its own that
consumes every partition directly from the start, with no group, and the meta line says
`"api":"direct"`. The two builds differ in timing too: v1.20.7 also stopped `ProduceSync` waiting
out the 10 ms linger that franz-go has defaulted to since v1.20.0, and on 2026-10-14, 100
one-at-a-time publishes took 0.44 s on v1.20.7 and 1.70 s on v1.20.6. Anything in the
calibration that depends on timing behaves differently between the two.

**Consumer groups.** A group member that takes over a partition starts from the group's committed
offset, so messages the previous owner received but hadn't committed arrive again. That is
at-least-once delivery, documented and legal, and Jepsen's own checker allows poll skips and
nonmonotonic polls when its consumers subscribe through a group. It is the durable-consumer
question from the open questions, arriving early. Until it's settled, consumers are assigned
partitions directly, with no group. That should promise what the NATS ordered consumer promises,
every offset once and in order, so the v0 rules apply; each client's docs get quoted when its
driver is written (principle 1).

## History format (the wire contract)

A history is one JSON object per line. The first line is a `meta` record, so every history says
exactly what produced it.

```jsonl
{"type":"meta","driver":"natsgo","client":"github.com/nats-io/nats.go@v1.53.1","api":"legacy","server":"nats-server 2.14.7","scenario":"slow-short","faults":["stall","overflow"],"params":{"n":100,"pending_msgs":20,"stall":"2s"},"driftlab":"<git sha>"}
{"index":0,"time_ns":0,"process":"producer","type":"invoke","f":"publish","value":1}
{"index":1,"time_ns":410000,"process":"producer","type":"ok","f":"publish","value":1,"stream_seq":1}
{"index":2,"time_ns":502000000,"process":"consumer","type":"ok","f":"deliver","value":1,"stream_seq":1}
{"index":3,"time_ns":502050000,"process":"nemesis","type":"invoke","f":"overflow"}
{"index":4,"time_ns":502100000,"process":"nemesis","type":"invoke","f":"stall"}
{"index":5,"time_ns":611000000,"process":"consumer","type":"info","f":"client-error","error":"nats: slow consumer, messages dropped"}
{"index":6,"time_ns":611100000,"process":"nemesis","type":"ok","f":"overflow"}
{"index":7,"time_ns":2502300000,"process":"nemesis","type":"ok","f":"stall"}
{"index":8,"time_ns":9000000000,"process":"final-read","type":"ok","f":"read","value":1,"stream_seq":1}
```

Field rules:

- `type` is one of `invoke`, `ok`, `fail` or `info`, following Jepsen's vocabulary. The only other
  value is `meta`. The NATS driver records every publish error as `info`, because nats.go promises
  nothing about them. The franz-go driver records them as `fail`: franz-go fails an idempotent
  record only when it knows the record was not written, since its Produce docs call a record whose
  request has been sent "not safe to fail". A failed value that turns up in the final read is then
  a phantom, and a broken promise.
- `value` is unique per message across the run: the producer assigns 1, 2, 3, and so on. This
  makes every message identifiable end to end, independently of the server.
- `stream_seq` is the server's sequence. It appears on `ok` publishes (taken from the PubAck), on
  every delivery (taken from the message metadata) and on final-read records.
- `partition` and `offset` take the place of `stream_seq` in a Kafka history, and always come as a
  pair. Both count from 0, so 0 is a real position here, unlike in `value` and `stream_seq`, and
  an `ok` publish, delivery or final-read record that has neither a pair nor a `stream_seq` is
  rejected rather than read as 0. A history uses one numbering or the other, never both.
- `poll` numbers a Kafka consumer's polls, from 1, on every `deliver` a poll returned, so the
  batch boundaries survive (see v1 design). The checker ignores it for now.
- `process` is one of `producer`, `consumer`, `nemesis` or `final-read`. Fault events live in the
  same history as everything else, so each anomaly can be lined up against the fault that caused
  it.
- `faults` appears only on the meta line and lists the faults the scenario sets out to inject. The
  nemesis records each fault as an operation, with `f` naming it: `invoke` when it starts and `ok`
  once it has fully happened. Faults are declared up front instead of inferred from the nemesis's
  own events, because a driver bug that never starts a fault would log nothing, and a rule inferred
  from the log would then demand nothing.
- A consumer records `subscribe` (an `invoke`, then an `ok`) when it starts. Without it, a consumer
  that received nothing would leave no trace at all, and the checker would judge it clean.
- The consumer also records `disconnect` (an `info`, with the error) when its connection drops,
  and `reconnect` when it comes back. They show when the client noticed a network or server
  fault; the checker ignores them.
- `time_ns` is monotonic time since the driver started, not wall-clock time. That follows from
  principle 4. Once one run has several processes, the harness will stamp receive times instead.
- Unknown fields are ignored, so the format can grow.

**Final read:** after the scenario ends, the driver reads every stream sequence back by number,
without a consumer, so the ground truth doesn't depend on the machinery under test. That read is
the ground truth for what the stream contains.

## Checker

The checker is a pure function, `Check(history) -> Result`, with no network access and no clocks.
Even liveness is judged by position in the history (principle 4).

| Anomaly | Definition | Usually whose bug |
|---|---|---|
| `lost-write` | an `ok` publish whose value is missing from the final read | server (durability) |
| `poll-skip` | the consumer jumps from stream_seq a to some b > a+1; the skipped range is then classified as *delivered late* or *never delivered* | client |
| `nonmonotonic-poll` | the consumer delivers b after a, with b < a | client |
| `duplicate-delivery` | one consumer receives the same stream_seq twice | client |
| `stall` | when the final read begins, the consumer hasn't yet received the stream's last sequence. Consumers are known from their deliveries and their `subscribe` operations, so one that received nothing is still judged | client (liveness) |
| `phantom` | a value seen by the consumer or the final read that no producer ever invoked, or whose publish was recorded as `fail` | client or server |
| `duplicate` | one value at more than one position in the final read: one write stored twice | client (producer) or server |
| `inconsistent-offsets` | an ack or a delivery places a value at a position where the final read found a different one | client or server |
| `nonmonotonic-send` | one producer's acks place a send at or before the send it made just before, in the same partition, taking sends in the order the producer made them | client (producer) or server |

In a Kafka history every rule applies within each partition, with offsets, which count from 0, in
place of stream sequences. A consumer has to reach the end of every partition the final read
returns, and a hole in any partition counts.

An `info` publish may or may not appear in the stream, and neither case is an anomaly. In v0 every
publish error is recorded as `info` (see the field rules), so only the first half of `phantom` can
fire until definite failures are classified.

**Verdicts.** A run is *clean* (no anomalies), *failed* (at least one anomaly) or *invalid*. A run
is invalid when a declared fault has no nemesis `ok`, meaning there's no evidence the fault fired.
Like a test strip whose control line never shows, it tested nothing, so it can't count as clean. A
run is also invalid when it has no final read, or when the final read has a hole that no acked
publish explains, because the ordered-consumer contract (principle 5) only holds for a contiguous
stream. A hole that an acked publish does explain is a lost write, a real finding against the
server, so it makes the run failed instead. Invalid outranks
failed: an invalid run's anomalies are still listed, but they aren't findings until the run is
repeated validly. Every verdict also reports how many publishes ended as `info`, so a clean verdict
with a large blind spot is visibly weak.

**Recovery.** When a run has faults, the checker also measures how long each consumer took, after
the last fault ended, to receive every stream sequence that had been acked by then, or reports
that it never did. The measurement is printed alongside the verdict and never feeds into it
(principle 4). It exists because of the first valid v1.54.0 runs. The fixed legacy consumer lost
nothing, but it recovered in 5-second steps, one per heartbeat, because each refetch overflowed
the tray again. Each step carried at least one tray of messages, and sometimes several, because
the callback keeps draining the tray while the refetch arrives. With 100 messages and a 2-second
stall, recovery took 3 seconds with a 40-message tray, 13 with 20, and 23 with 10. One 20-message
run took 15 seconds, and its clean verdict cleared the grace window by milliseconds. The
`jetstream` API recovers without steps. With the same stall it caught up in 3 milliseconds. With
1,000 messages and a 20-message tray, the legacy consumer took 23.061 seconds to catch up, and
`jetstream` took 34 milliseconds.

**Lag.** Recovery only counts messages acked before the last fault ended, so it can't see a
consumer that is slow with messages acked after that. The checker therefore also reports, for each
consumer in a run with faults, the longest any acked message waited between its ack and the
consumer's first delivery of it. That is what a user of the consumer feels, how stale its view of
the stream got, wherever the faults fell. A publish that ended as `info` has no ack time, so it
doesn't count, and neither does a message the consumer never received, which the stall and
poll-skip anomalies already cover. Like recovery, lag never affects the verdict. The
`deaf-after-fault` fixture shows why it exists: in the same history recovery reads 0s and lag reads
13.801s, because the only message acked before the fault ended had already arrived.

Each anomaly carries the history indexes of the ops involved, so the output points at evidence
instead of just summarising it. Attaching the `client-error` and nemesis events that fall inside an
anomaly's window is planned, not built.

## Architecture

```
laptop   edit code, run checker unit tests (pure Go), git push
           |
           v   git pull
newpc    docker compose: nats-server   <---   driver binary (one per client version)
                                                  |
                                                  v  stdout
                                             history.jsonl  --->  driftlab check  --->  verdict
```

- **Drivers are separate binaries that write the JSONL format to stdout.** They share no Go code
  with the checker, because the history format *is* the interface. That is what lets two nats.go
  versions sit behind one checker, and later sarama vs franz-go, or a client written in another
  language. It also means a driver that panics can't take the checker down with it.

### Why two go.mod files

Go builds a module with exactly one version of each dependency, so a single module can never link
nats.go v1.53.1 and v1.54.0 together. That one rule decides the layout.

- **The root `go.mod` belongs to the judge.** The checker and the CLI import no client library at
  all. That keeps them buildable on the laptop's Go 1.25, and it means upgrading a client library
  can never change how histories are judged.
- **`drivers/natsgo/go.mod` belongs to the witness.** The driver is the only code that imports
  nats.go. It pins v1.54.0 and needs Go 1.26, because nats.go v1.54.0 declares `go 1.26.0`, so it
  gets built on newpc. A separate module keeps both of those out of the checker.
- **`drivers/natsgo/v1.53.1.mod` is not a third module.** It is a second recipe for the same
  driver: the same source code with the older nats.go. `go build -modfile=v1.53.1.mod` uses it, and
  Go keeps its checksums in a matching `v1.53.1.sum`. So one source tree produces two binaries.
  `go version -m <binary>` proves which nats.go got linked, and the driver also writes that version
  into its `meta` line.
- **The two modules never import each other.** They only share the JSONL format. So the repo needs
  no `go.work` file, and `go build ./...` at the root skips `drivers/`, because a folder with its
  own `go.mod` belongs to that module instead.
- **In v1, every Kafka client gets its own driver module**, so their dependency trees can never
  collide.

## Command line

`driftlab check <history.jsonl>` prints the verdict, the invalid reasons, the anomalies and, in a
run with faults, each consumer's recovery and lag, one per line. The exit code is the verdict for
scripts: 0 clean, 1 failed, 2 invalid, and 3 when the command couldn't run (bad usage, or a file it
can't open or parse). An invalid run exits non-zero on purpose, so a script can never mistake a
broken experiment for a pass.

## Repo layout

```
driftlab/
├── DESIGN.md
├── go.mod
├── cmd/driftlab/
├── internal/
│   ├── history/
│   └── checker/
│       └── testdata/
├── drivers/
│   ├── natsgo/
│   │   ├── go.mod
│   │   ├── v1.53.1.mod
│   │   └── repro2180/
│   └── franzgo/
│       ├── go.mod
│       └── v1.20.6.mod
├── deploy/
│   ├── compose.yaml
│   └── kafka.yaml
└── scripts/
    ├── matrix.sh
    └── kafka-calibrate.sh
```

| Path | What it is |
|---|---|
| `go.mod` | the root module: the checker and the CLI, with no client libraries |
| `cmd/driftlab/` | the CLI: `driftlab check <history.jsonl>` |
| `internal/history/` | the Op type and the JSONL reader |
| `internal/checker/` | `Check()` and its anomaly kinds |
| `internal/checker/testdata/` | hand-written histories: one clean, one per anomaly kind, and more for the verdict rules and the measurements |
| `drivers/natsgo/` | a separate module that talks to NATS and writes a history to stdout |
| `drivers/natsgo/go.mod` | pins nats.go v1.54.0 (the fixed release) |
| `drivers/natsgo/v1.53.1.mod` | pins nats.go v1.53.1 (the last buggy release), used via `-modfile` |
| `drivers/natsgo/repro2180/` | a standalone reproducer for nats.go issue 2180, which needs only nats-server |
| `drivers/franzgo/` | a separate module that talks to Kafka through franz-go and writes a history to stdout |
| `drivers/franzgo/go.mod` | pins franz-go v1.20.7 (the release that fixed issue 1217) |
| `drivers/franzgo/v1.20.6.mod` | pins franz-go v1.20.6 (the last release before the fix), used via `-modfile` |
| `deploy/compose.yaml` | nats-server with JetStream (weekend 2), toxiproxy (weekend 3) |
| `deploy/kafka.yaml` | three Kafka 4.3.1 brokers for v1, in a compose project of their own |
| `scripts/matrix.sh` | runs the full matrix (2 versions × 2 APIs × 8 scenarios) and checks every verdict against the table |
| `scripts/kafka-calibrate.sh` | runs v1's calibration (2 franz-go versions × 2 scenarios × 3 repeats) and checks every verdict against the expected one |

## Milestones

- **2026-09-25 (done):** the repo, this document, go.mod and .gitattributes, pushed.
- **Weekend 1 (done 2026-09-27; the checker, no NATS at all):** the history reader, `Check`, the verdict rules, and
  table-driven tests over hand-written histories. The fixtures are one clean history, one per anomaly kind, plus
  `info` publishes that are present and absent (both must pass). Exit: `driftlab check` flags
  every bad fixture and passes every good one. That is calibration step (a).
- **Weekend 2 (done 2026-10-06; the driver and calibration):** the natsgo driver (both APIs, both
  versions), the compose file and the matrix above. Exit: the matrix reproduces, which it did on
  the first full run of `scripts/matrix.sh`. That is calibration steps (b) and (c).
- **Weekend 3 (done 2026-10-13; network and server faults):** toxiproxy on the consumer's line for
  a slow line and a cut line, docker pause and kill for the server, the lag measurement, and the
  32-run matrix. It found the first things nobody had reported: nats.go issues 2180 and 2181 (see
  Weekend 3 results).
- **v1 (started 2026-10-13; the Go Kafka clients):** the three-broker cluster in
  `deploy/kafka.yaml`, explored by hand, and the decisions in v1 design. First exit: the
  calibration pair reproduces, with franz-go v1.20.6 failing and v1.20.7 passing. The version
  ends with the published table.

## Beyond v0 (one line each, so v0 stays small)

Each version ends with something public, a finding or a published results table, before the next
one starts.

- **v1:** the Go Kafka client matrix (franz-go, sarama, kafka-go, confluent-kafka-go) against a
  three-broker cluster, with consumer-group rebalances and leader elections (see v1 design). Then
  Redpanda and Redis Streams. The output is a published table of client × documented guarantee ×
  verdict under each fault.
- **v2:** backpressure, exactly-once claims, redelivery semantics, time-to-recovery.
- **v3:** seeded fault schedules, shrinking a failing schedule down to a minimal one, and a CI mode
  so maintainers can run it nightly.
- **v4:** each client's documented guarantees encoded as machine-readable claims files.

## Open questions (decided when we reach them)

- **Batch deliveries (decided 2026-10-13).** One `deliver` line per message, with a `poll`
  number, and Kafka positions in new `partition` and `offset` fields. See v1 design.
- **Orchestration (decided 2026-10-06, revised 2026-10-13).** A shell script, `scripts/matrix.sh`,
  runs the v0 matrix. Weekend 3 didn't need more: each driver injects its own faults and records
  them in its history, so a run stayed "run the driver, then judge the history". v1 starts the
  same way, with one client library in two versions, so a Go `driftlab run` command now comes
  with v3, when fault schedules have to be generated, re-run and shrunk.
- **Durable consumers.** With acks, receiving a message twice becomes legal, but only as a
  redelivery. NATS marks its redeliveries; Kafka doesn't, so the checker will have to work them
  out from a group's assignments and committed offsets. That's a different contract, so it
  probably needs a separate checker mode. *Comes up:* in v1, earlier than planned, because a
  Kafka consumer group redelivers after a rebalance (see v1 design).
