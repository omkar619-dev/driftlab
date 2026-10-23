# driftlab: design

> Status: last updated 2026-10-14. v0 is done: the checker, and one system (NATS JetStream, Go
> client) under stall, network and server faults. The calibration matrix reproduces, and weekend 3
> produced two upstream reports (see Weekend 3 results). v1, the Go Kafka clients, has a working
> franz-go witness, and its calibration pair reproduces (see v1 calibration results).

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
glossary. The other names (stall, phantom, duplicate-delivery, nonmonotonic-copy) are driftlab's
own. `lost-write` is judged differently. Jepsen infers it from reads: an acked message that nobody
read, although someone read a later offset; an acked message nobody read at all is `unseen`.
driftlab checks each acked message against a final read of the whole log instead, so it can tell
a message the log lost (a lost write) from one the log kept but a consumer never got (a stall or a
poll-skip), and it also catches a message that a consumer received before the log lost it.

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
  https://github.com/nats-io/nats.go/issues/2181, and fixed by
  https://github.com/nats-io/nats.go/pull/2182, merged on 2026-10-19, after v1.54.0.
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

**v1 calibration results.** On 2026-10-14, at commit `0c748d9`, against apache/kafka 4.3.1, all 12
runs of `scripts/kafka-calibrate.sh` came out as expected:

| Scenario | franz-go v1.20.6 | franz-go v1.20.7 |
|---|---|---|
| `deadline-control` | clean ×3 | clean ×3 |
| `followers-pause` | **failed** ×3: lost-write, inconsistent-offsets, phantom | clean ×3 |

- Every v1.20.6 failure has the same fingerprint. Value 51, sent into the freeze, failed with
  `context deadline exceeded` about 10 seconds later, yet the final read has it at offset 50.
  Value 52 failed the same way and was never written. Value 53 was acked at offset 50, where 51
  sits, and is missing from the final read.
- The fix is visible in the diary. In both versions the broker answers REQUEST_TIMED_OUT about 10
  seconds after the send. v1.20.6 fails the record at that moment; v1.20.7 holds it until the
  freeze ends, about 25 seconds after the send, and then acks it at its real offset.
- The first full run, at `bc23bc1`, came back invalid: all six freeze runs were missing the
  timeout evidence. The watcher listened for an info-level line that franz-go writes only for a
  batch that ends in an error, not for a REQUEST_TIMED_OUT it retries. As with v0's first stall
  runs, the invalid verdict caught a bug in the harness rather than calling the runs clean.
  Those diaries show the same failures, and are kept.

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

**sarama.** `drivers/sarama` tests IBM's sarama the same way: sarama writes and, in `control`, reads
while the topic is written, and franz-go v1.20.7 does everything else, from creating the topic to
the final read, so the witness is never the thing being tested. sarama's defaults leave its
idempotent producer off, so the driver turns it on, with the three settings sarama requires for it:
acks from every in-sync replica, one request in flight per broker, and at least one retry, which the
default of three already gives. Its docs promise that the idempotent producer "will ensure that
exactly one copy of each message is written", and the checker holds it to that; the same doc says it
keeps messages in order more strictly, which `nonmonotonic-send` checks. Two more settings only
serve the driver: each success comes back with its position, and each message names its partition.
Everything else stays at sarama's defaults, including the protocol version it speaks, 2.8, except
where a scenario below changes it, and the meta line records each setting a scenario can change. The
driver writes through `AsyncProducer`, at a steady pace and without waiting for each ack, so several
values are in flight at once, as they are for most sarama users. A success is recorded as `ok`, with
the partition and offset the broker gave. An error is recorded as `info`, not `fail`, because sarama
only says the producer failed to deliver the message, which doesn't promise that the broker never
stored it. In `control` the helper is a `PartitionConsumer` on every partition, reading from the
oldest offset with no group; sarama hands messages over one at a time, so its deliver lines carry no
poll number.

**The sarama calibration pair.** Before https://github.com/IBM/sarama/pull/3762, sarama numbered
each idempotent message as it queued it for a broker, not when its batch was sent. So a message
numbered before a failed batch bumped the producer's epoch could be sent under the new epoch with
its old sequence number, a later message could share its identity, and the broker would acknowledge
that one as a duplicate without writing it: the same shape as franz-go 1217. The pull request's two
commits sit between v1.61.0 and v1.61.1, but so do 44 others, among them more producer fixes. After
https://github.com/IBM/sarama/pull/3768, for one, a failed batch bumps the epoch once instead of
once per message. So a failure that v1.61.0 has and v1.61.1 doesn't can't be pinned on 3762. The
calibration pair is two commits instead: d0755db, just before the pull request, and 477992f, its
last, which differ by the pull request alone. `pr3762-before.mod` and `pr3762-after.mod` pin them.
`v1.61.0.mod` and `go.mod` pin the releases, which stay in the runs because they are what people
install. All four keep the compression libraries at the versions v1.61.1 needs, so any two of the
builds differ only in sarama.

**The 3762 scenario.** The bug needs a batch that uses up its retries, because only then does sarama
start a new epoch. In `followers-cut` the topic has one partition, and sarama's `Producer.Timeout`,
how long the leader may wait for its copies, is 2 seconds instead of 10. Just before value 201 the
driver cuts the fetches of the two brokers that hold the partition's other copies, with the nftables
cut of `relay-restart-cut`, for 20 seconds. The leader stores each batch but can't get it copied,
answers REQUEST_TIMED_OUT after 2 seconds, and once sarama's three retries are used up, about 8
seconds in, the batch fails and the epoch moves on. With the default 10 seconds the retries would
outlast the cut, and nothing would fail. It is a cut, not the freeze of franz-go's calibration,
because sarama's idempotent producer refreshes its metadata after every retriable error, on the
producer's own goroutine, from whichever broker is least busy. With two of three brokers frozen that
is usually a frozen one, which only answers after the thaw, so no batch would use up its retries
during the freeze. That comes from reading sarama's code, not from a run. A cut broker keeps
answering clients. With the few seconds nft takes to put the cut in place, 20 seconds stays well
under the 30 after which the leader drops a lagging follower. The stream runs at the usual pace for
3,200 values, so it goes on for about 10 seconds after the cut, and new messages get numbered
before, during and after each new epoch. The run declares two faults, each with evidence of its own:
the cut (nft lists the rule in both brokers' networks) and the timeout (a publish that failed with
REQUEST_TIMED_OUT after the cut began). Without both, the trigger didn't fire, and the run is
invalid rather than clean. When the cut ends, its `ok` records the in-sync set as the leader saw it.
A message the bug swallows shows up as a lost write and as an inconsistent offset where it was
acked, and as a nonmonotonic send when its ack lands at or before the offset of the message sent
just ahead of it. `timeout-control` is the same run without the cut, and `followers-cut-single` is
the same run with one message per request (sarama's `Producer.Flush.MaxMessages` set to 1). A
message is only lost when a fresh batch carries exactly the numbers of one the broker has just
stored. With sarama's normal batching the counts rarely line up, and a batch sent with old numbers
is turned away as out of order instead: a failure a correct client may report, so the checker calls
such a run clean. One message per batch lines the numbers up, as franz-go's one-at-a-time sends did
for 1217. `scripts/sarama-calibrate.sh` runs the three scenarios three times on all four builds. It
expects only `followers-cut-single` on the two builds without 3762 to fail, and it counts each run's
publishes turned away as out of order, which only those two builds should have.

**sarama calibration results.** On 2026-10-19, at commit `ef7a8a3`, against apache/kafka 4.3.1, all
36 runs of `scripts/sarama-calibrate.sh` came out as expected:

| Scenario | d0755db, before 3762 | 477992f, after 3762 | v1.61.0 | v1.61.1 |
|---|---|---|---|---|
| `timeout-control` | clean ×3 | clean ×3 | clean ×3 | clean ×3 |
| `followers-cut` | clean ×3; 415–1226 out of order | clean ×3; none out of order | clean ×3; 416–1246 out of order | clean ×3; none out of order |
| `followers-cut-single` | **failed** ×3: lost-write, inconsistent-offsets, nonmonotonic-send | clean ×3 | **failed** ×3: the same | clean ×3 |

- Every failure has the same fingerprint: two messages swallowed back to back. In the first
  `followers-cut-single` run on d0755db, values 1551 and 1552 were stored at offsets 619 and 620.
  Values 1553 and 1554, sent just after 1552, were acked at offsets 619 and 620 as well, and are
  missing from the final read: the broker answered them as repeats of 1551 and 1552, the shape PR
  3762 describes. All four acks came back within 2 milliseconds. Only 1553 breaks the send order,
  because 1554 comes after it both in sending and in offset. Kafka's own console consumer read the
  topic again and agrees with the final read at all 2267 offsets.
- With sarama's normal batching, the same two builds lost nothing, in these 6 runs and in the 6 of
  the first pass. Each of those 12 runs instead had 415 to 1269 publishes turned away as out of
  order, and no run on the fixed builds had any. sarama reports them as failed sends, which its
  promise allows, so the checker calls those runs clean, and only the out-of-order count tells the
  builds apart.
- On the builds without the pull request, most failed sends are those rejections, not timeouts:
  about 715 of about 936 in a `followers-cut-single` run, where the fixed builds fail 921 sends, all
  of them timed out.
- The releases behave like the commits on either side of the pull request in every scenario: v1.61.0
  like d0755db, and v1.61.1 like 477992f. The two commits differ by the pull request alone, so in
  these runs it accounts for the whole difference between the releases.
- The first pass got the expectations wrong. At `1b52108` the script still expected `followers-cut`
  itself to fail on the builds without 3762, and 6 of 24 runs came back not as expected: with normal
  batching, the bug never lined a stale batch up with a fresh one. Two more passes at that commit
  with `-max-messages 1` failed all 12 runs on those builds and passed all 12 on the fixed ones,
  which is why `followers-cut-single` exists. Counting this pass, one message per request has failed
  all 18 runs on the builds without 3762 and passed all 18 on the fixed ones, and all 48 control
  runs were clean. As with franz-go's first full run, the miss was in what the harness expected, not
  in the checker, and those diaries are kept.

**sarama's table.** After the calibration, the same driver runs the faults of the published table,
on v1.61.1 with the idempotent settings above, each on a one-partition topic, so every message meets
the fault, and with a solo helper reading through sarama's partition consumer, whose docs say it
keeps retrying through every error but an offset out of range. `leader-kill` kills the broker that
leads the partition just before value 501, the way a crash would, and starts it again 20 seconds
later. Until the controller notices, after the brokers' 9-second session timeout, and picks a new
leader from the in-sync set, sarama has nowhere to send, and its three retries, 100 milliseconds
apart, run out long before; those sends end as `info`. The kill only counts if docker shows the
broker stopped and the cluster then names a new leader. Its `ok` comes when that leader shows up,
the moment the partition can be used again, so the helper's recovery is measured from there, and it
records how long the election took. The broker comes back as a follower, and the run waits until it
is back in the in-sync set, so the next run starts with three brokers; every run also waits for the
cluster to list three live brokers before it creates its topic.

`followers-pause` freezes the partition's two followers with `docker pause` for 25 seconds, as
franz-go's calibration does. The leader can't get anything copied, so after `Producer.Timeout`, 10
seconds, it answers REQUEST_TIMED_OUT, and sarama refreshes its metadata before it retries, from
whichever broker is least busy. If that is a frozen one, the producer waits there until the thaw,
and so does every message behind it: the stall the 3762 scenario predicts from reading sarama's
code. A frozen broker, unlike a cut one, doesn't answer clients either. `-sarama-log` writes
sarama's own log, debug lines included, with UTC times to the microsecond and the history's time 0
on its first line, so that wait shows as the gap between a `client/metadata fetching metadata` line
and the next `producer/broker` line. A freeze run first thaws any broker a stopped run left frozen.

`follower-pause` and `follower-cut` hit only one of the two followers, as a broker that hangs or
loses its link to the leader would. A frozen follower stops checking in, so with the other two
brokers still a quorum, the controller fences it after the 9-second session timeout, about 12
seconds when it was the active controller and a new one has to be picked first, and fencing takes it
out of the in-sync set. A cut follower keeps checking in, so only the leader drops it, once it has
lagged for 30 seconds; the leader looks every 15 seconds, so that happens 30 to 45 seconds in, and
the cut holds for 50. Either way the leader then counts a write as copied by the other follower, and
the partition takes writes again; an `info` line records when the in-sync set shrank, measured from
the fault's start. Run with `-timeout 2s`, `follower-pause` also tests what `followers-pause`
showed: refreshing its metadata after each timed-out batch, sarama's producer can end up waiting on
the frozen broker after the partition took writes again. Only the two calibration scenarios, marked
`expectTimeout`, now declare the `timeout` fault, so a cut that never times a batch out isn't called
invalid.

**The metadata stall.** The targeted runs confirmed it. On 2026-10-19, at commit `3a3342f`, six
`follower-pause` runs with `-timeout 2s -hold 60s` all came out clean, but in four of them the
producer asked the frozen broker for metadata and waited out the whole 30-second
`Net.ReadTimeout`: the partition took writes again 7.8 to 8.6 seconds into the freeze, and
acknowledgements came back at 32.2 seconds. In the other two it never asked the frozen broker, and
resumed with the partition. With one request allowed per connection, every broker reports nothing
pending, so the least busy broker is just the first in Go's map order, which was broker 1 in 15 of
17 requests: the runs stalled exactly when broker 1 was the frozen one. `drivers/sarama/repro3843`
shows it with mock brokers and no cluster: 6 of 10 sends wait out the read timeout, on v1.61.1 and
on sarama's main at `f34447e`. Reported as https://github.com/IBM/sarama/issues/3843.

**sarama's defaults.** `-defaults` runs the same rows with the producer as sarama ships it, as most
of its users run it: acknowledgements from the leader alone (`WaitForLocal`), no idempotence, and up
to 5 requests in flight per connection. The driver still turns on `Return.Successes` and its manual
partitioner, and the meta line records which settings a run used. sarama promises neither one copy
nor, its `Net.MaxOpenRequests` docs say, order with those settings, and Kafka's docs for `acks=1`
say a record is lost if the leader fails after acknowledging it but before a follower has copied
it. So in this column the checker's anomalies are observations of documented behavior, not broken
promises. Where a fault shows up should move too: with the leader answering alone, a frozen or cut
follower can't hold the writer up, only the reader, which gets a message once it is copied, and a
killed leader can take messages it acknowledged with it. v1.61.1's producer still keeps one batch
in flight per partition whatever `Net.MaxOpenRequests` says, so on a one-partition topic the order
should hold.

**The live kill.** Until now every fault started on the driver's main loop, just before value 501,
so the writer handed sarama nothing while docker acted on the broker: a tenth of a second for a
freeze and about 0.6 seconds for a cut, measured as the writer's first gap in the `-defaults` runs.
By the time a killed leader died, every message it had acknowledged had been copied, so
`leader-kill` can't show what Kafka's docs for `acks=1` describe. A scenario can now start its fault
live, on a goroutine of its own, and set its own pause between values; `-interval` keeps the
scenario's pause, 10 milliseconds unless it sets one, when it isn't given. `leader-kill-live` kills
the leader that way while writing a value every millisecond, 20,000 values, so the writer is still
sending when the leader dies and writes again once a new one is picked. With `-defaults` it should
lose the few messages acknowledged in the leader's last milliseconds, each a `lost-write`, with an
`inconsistent-offsets` once the new leader gives its offset to a later message. With the idempotent
settings, which wait for every in-sync copy, nothing acknowledged should be lost. The meta line
marks a live fault, along with the interval the run used.

**kafka-go.** `drivers/kafkago` tests segmentio's kafka-go the same way, with franz-go again as the
witness. Its `Writer` has no idempotent producer: it never asks for a producer ID, so a retry after
a lost reply can store a message twice, which its docs also say about a cancelled write. Its
strongest setting is acknowledgements from every in-sync copy (`RequireAll`), the driver's default;
`-acks one` asks for the leader alone, and `-acks none` keeps kafka-go's own default,
`RequireNone`, which every `Writer` in its README uses and which asks for no acknowledgement at
all. With none, Kafka sends no reply, so kafka-go has no offset to report, and the driver records
each send as an `info`: whether it was stored is unknown, and the final read is the measure. The
writer runs asynchronously, with a `Completion` callback that reports each batch's outcome and the
offset of every message in it. Each message carries its value in `WriterData`, and a small
balancer sends value v to partition (v-1) mod the partition count. Otherwise the writer keeps
kafka-go's defaults: one batch at a time per partition, sent at 100 messages or after a second,
with up to 10 attempts. The helper is a kafka-go `Reader` per partition, with the partition set and
no group. Before the final read, the driver waits for the helper to reach the end of every
partition, as the witness sees it. Its first version waited for the last acknowledged offset
instead, so with none it didn't wait at all, and the checker reported a stall in all three
partitions: the helper had read one message of the 100 when the final read began. kafka-go can't be
calibrated the way franz-go and sarama were, on a fixed bug in an idempotent producer, because it
has none, and its fixed bugs that lose data are in consumer groups, which v1 doesn't use, or in
reading past compacted records, which v1 doesn't write. Instead it is checked on what Kafka
documents, with the driver's own `leader-kill-live`, which works as drivers/sarama's does, with the
kill always started live: `-acks one` must lose acknowledged messages and `-acks all` must not.
While the partition has no leader, kafka-go retries the batch at the head of the partition's queue
when the error looks temporary, up to 10 times and at most a second apart, about 7 seconds in all,
and the values behind it wait in the queue. It hears of the new leader only at its next metadata
refresh, which comes at a random moment within 6 seconds of the last one; a failed write doesn't
bring it forward.

**kafka-go's freezes and cuts.** The kafka-go driver also runs sarama's `followers-pause`,
`follower-pause` and `follower-cut`, with the fault started live like its kill. With `-acks all`,
kafka-go asks the leader to wait at most 5 seconds, half its 10-second write timeout, for every
in-sync copy. While a frozen or cut follower still counts as in sync, the leader answers
REQUEST_TIMED_OUT, and kafka-go sends the same batch again. Given no producer ID, the leader can't
tell it from a new batch and stores it again, which Kafka's docs say a producer without idempotence
may do when it retries. So each attempt should leave one more copy of the batch, a `duplicate` for
every value in it. With `-acks none`, nothing waits and nothing is sent twice.

**Consumer groups.** A group member that takes over a partition starts from the group's committed
offset, so messages the previous owner received but hadn't committed arrive again. That is
at-least-once delivery, documented and legal, and Jepsen's own checker allows poll skips and
nonmonotonic polls when its consumers subscribe through a group. It is the durable-consumer
question from the open questions, arriving early. Until it's settled, consumers are assigned
partitions directly, with no group. That should promise what the NATS ordered consumer promises,
every offset once and in order, so the v0 rules apply; each client's docs get quoted when its
driver is written (principle 1).

**Relay runs.** Some programs copy messages from one Kafka cluster to another, such as relays and
mirrors. Testing one takes two clusters: a one-broker source, `deploy/kafka-source.yaml`, and the
three brokers of `deploy/kafka.yaml`, which hold the copy. The writer's acks then come from the
source and the final read is of the copy, so a position on one side says nothing about the other.
The meta line declares such a run with `"relay":true`. `lost-write`, `duplicate` and `phantom`
already judge by value, so they keep their meaning: every value the source acked must be in the
copy exactly once, and nothing else may be. `inconsistent-offsets` is switched off, because it
compares an ack's position with the final read's: after a single duplicate, every later message in
the copy sits one offset further along, and it would report every one of them. In its place,
`nonmonotonic-copy` checks order: in each partition of the copy, the first copies of the values
from one source partition must sit in the order the source stored them. Repeats are left to
`duplicate`, and values from different source partitions may mix, because the source never ordered
them against each other. Which partition of the copy a value lands in isn't judged yet. The
source's acks can't explain a hole in the copy, so any hole makes a relay run invalid. Recovery and
lag aren't measured in relay runs yet, because both match acks to deliveries by position.

**The relay witness.** `drivers/relay` treats the program under test as a black box. It creates
the same fresh one-partition topic on both clusters, starts the program with a command it is given
plus the two topic names, and writes numbered values into the source at a steady pace, each acked
by the source. It keeps the program running the way systemd or Kubernetes would: whenever the
program exits without being asked to, the driver records the exit and starts it again, at once if
it had run for a while, and after a second if it died within three seconds of starting. Its own
reads and writes use franz-go v1.20.7, whatever the program under test links, so the witness is
never the thing being tested. Once the copy has caught up, or a grace window has passed, it stops
the program with SIGTERM, as an operator would, and kills it if it is still running 30 seconds
later, which the history records. Then it reads the copy back. In `relay-pause`, once the copy has
caught up with the first half of the writes, it freezes the two brokers that hold the copy's other
replicas for 25 seconds, as `followers-pause` does. That run declares two faults: the freeze, with
docker's word that both brokers were paused, and a stall, which completes only if the copy's end
stayed put for 10 seconds while the source was ahead of it. A freeze that missed the copy makes the
run invalid rather than clean. When a fault ends, its `ok` records the partition's in-sync set as
the leader saw it just before. `relay-control` is the same run without the freeze.

`relay-restart` freezes only one of the two followers, for 25 seconds. The cluster keeps its
controller quorum, so a program started during the freeze can still reach it, but the controller
also fences the frozen broker once its session runs out, about 9 seconds in, and that takes it out
of the in-sync set. Until then the leader can't count anything as copied, so this stall only has to
last 2 seconds. Then the driver kills the program with SIGKILL, as a crash would, and the
supervisor starts it again. That third fault, the crash, only counts if a new program then runs for
three seconds while the copy's end still stands where it stalled. That shows the new program had a
stale end to read, not that it read it: one that is slow to start can read the end after it has
moved, so a clean `relay-restart` run says less than a failed one. After the freeze, once the copy
has caught up, the driver restarts the program politely and writes half as many values again,
which shows whether the restart skips any.

`relay-restart-cut` is the same run with a fault that keeps the copy's end stale for all 25
seconds. Instead of freezing a follower, it cuts that follower's fetches: a throwaway container
built from `deploy/nft` joins the follower's network namespace and adds an nftables table whose
one rule drops every packet the follower sends to port 19092, where the brokers fetch from each
other. The follower keeps checking in with the controller on port 9093 and keeps answering
clients, so it is never fenced, and the leader only drops it from the in-sync set once it has
lagged for 30 seconds (`replica.lag.time.max.ms`), longer than the cut lasts. That is closer to a
follower with a slow disk or a broken link to the leader than a frozen broker is, and a program
that restarts during the cut never meets a broker that doesn't answer. The cut only counts if nft
lists the rule in the follower's network, and deleting the table ends it.

## Relay results: kaf-relay

The first copier tested is kaf-relay (https://github.com/zerodha/kaf-relay), which keeps Kafka
clusters in sync by copying topics from one to another. It ran at main `2dcedd3` (v2.1.0), in
`--mode single`, with the settings in its `config.sample.toml` apart from SASL, which is off, and
the target's `request_timeout`, raised from 100 ms to 1 s. It keeps no bookmark of its own: every
start resumes the source at the copy's end offsets, and its README says that offset tracking
between source and target allows de-duplication without external stores. It pins franz-go
v1.20.6, and it was also built against v1.20.7 and v1.22.2 through a modfile, to tell what it does
itself from what its client does. The runs took place on 2026-10-16 and 2026-10-17, at driver
commits `6604a58` to `3ca91b6`:

| Scenario | franz-go v1.20.6 | franz-go v1.20.7 | franz-go v1.22.2 |
|---|---|---|---|
| `relay-control` | clean | not run | not run |
| `relay-pause` | clean ×6 | clean ×6 | clean ×6 |
| `relay-restart` | **failed** 2 of 6 | **failed** 4 of 6 | **failed** 1 of 6 |
| `relay-restart-cut` | **failed** 4 of 4 | **failed** 3 of 3 | **failed** 3 of 3 |

- **A restart while the copy can't commit duplicates a message, and the next restart then skips
  one, with no error.** Every failed run has the same fingerprint: value 101 at copy offsets 100
  and 101, and value 201 missing. The killed program had stored value 101 on the leader, which
  couldn't count it as copied while a follower in the in-sync set wasn't fetching. Its
  replacement read the copy's end, the high watermark, as 100 and copied value 101 again, under a
  new producer ID, so the broker couldn't tell it was a retry. From then on the copy was one
  message ahead of the source, so the polite restart resumed one offset too far and value 201 was
  never copied. kaf-relay logs where each start resumes: every start that resumed at 100 produced
  the failure, and every one that resumed at 101 was clean. The second witness, Kafka's console
  consumer, read the same layout from three failed runs' topics. The franz-go version makes no
  difference. Reported as https://github.com/zerodha/kaf-relay/issues/49.
- **The cut hits that window every time, the freeze only sometimes.** The controller fences a
  frozen broker once its session runs out: 7.6 to 9.4 seconds into the freeze, by its own log, or
  about 11 when the frozen broker was the active controller and a new one had to be elected
  first. A program restarted about 2.4 seconds in only read the stale end if its first requests
  avoided the frozen broker. When they didn't, its connection check waited out a 6-second
  timeout, or its first `ListEndOffsets` failed after 10 seconds with a dial timeout, although
  kaf-relay gives that call 1 second, and the program exited until the supervisor started it
  again. A cut follower keeps answering, so a restart reads the end at once, and the window lasts
  all 25 seconds.
- **A franz-go request can wait out another request's dial, past its own deadline.** Each broker
  handles its requests one at a time, and a request waiting its turn doesn't look at its context,
  which is most likely why the 1-second call above took 10 seconds: an earlier request was still
  dialing the frozen broker. `drivers/franzgo/repro1505` shows it with no broker at all. On its
  own, a request with a 1-second deadline returns after 1 second; sent 200 milliseconds after one
  with a 15-second deadline whose dial hangs, it returns after 9.8 seconds, when that dial gives
  up. The same on v1.20.6 and v1.22.2. Reported as https://github.com/twmb/franz-go/issues/1505.
- **kaf-relay survives franz-go issue 1217.** In `relay-pause` the trigger fired on v1.20.6, with
  three timed-out writes per run, but kaf-relay retries exactly the records that failed, and
  v1.20.6 rewinds a failed batch's sequence numbers, so the retry carried the same numbers and the
  broker matched it to the records it already had. It costs time: the copy caught up 5.9 to 8.5
  seconds after the freeze ended, against about 0.1 seconds on v1.20.7 and v1.22.2.
- **A relay doesn't start on a copy that has caught up, and retries fetch errors with no
  backoff.** Found by hand on 2026-10-16: with as many messages in the copy as in the source, it
  waits for the source to grow, and with `--stop-at-end` it never exits. Both come from one
  comparison in its failover code. Reported as https://github.com/zerodha/kaf-relay/issues/48.
  The hand tests also ran into a bug someone had already reported, every message landing in
  partition 0 of the copy (https://github.com/zerodha/kaf-relay/issues/46); the proposed fix,
  https://github.com/zerodha/kaf-relay/pull/47, was checked here and works for a fresh copy. The
  relay runs use one-partition topics, so it doesn't affect them.
- **The invalid verdict caught the harness three more times.** The first `relay-pause` runs read
  the copy's end through a broker picked at random, which a freeze can leave unanswered, so two
  of nine never saw the stall; the driver now asks the leader directly. The first `relay-restart`
  runs, at `f6b4d7a`, all came back invalid, because the stall threshold assumed the leader would
  wait about 30 seconds for the frozen follower, when the controller fences it after about 9;
  each scenario now has its own threshold. And clean `relay-restart` runs whose crash counted
  showed that the crash's evidence proves less than this document first claimed, which it now
  says.

## sarama results

sarama's table ran on v1.61.1 against apache/kafka 4.3.1, each fault on a one-partition topic with
a solo helper reading, once with the idempotent settings and once with sarama's own defaults
(`-defaults`). The runs took place from 2026-10-19 to 2026-10-21, at driver commits `93188d0`
(`leader-kill`), `c9d4441` (`followers-pause`), `3a3342f` (`follower-pause` and `follower-cut`),
`ccf3e42` (the defaults) and `bf77e5f` (`leader-kill-live`). Times are measured from the start of
the fault:

| Scenario | Idempotent settings | sarama's defaults |
|---|---|---|
| `leader-kill` | clean ×3; acknowledgements stopped for 9.2 to 14.0 s | clean ×3; acknowledgements stopped for 10.4 to 16.4 s |
| `leader-kill-live` | clean ×3 | **failed ×5**: 1, 526, 1, 524 and 521 acknowledged messages lost |
| `followers-pause` | clean ×3; the writer waited 25.2 s, the whole freeze | clean ×3; the writer never waited, the reader waited 25.2 s |
| `follower-pause` | clean ×3; the writer waited 7.8 to 9.3 s, or 25.2 s in one run (the metadata stall) | clean ×3; the writer never waited, the reader waited 8.0 to 10.1 s |
| `follower-cut` | clean ×3; the writer waited 34.6 to 40.1 s | clean ×3; the writer never waited, the reader waited 30.8 to 40.2 s |

- **The settings decide who waits, not how long.** With the idempotent settings the leader
  acknowledges a write only once every in-sync copy has it, so a frozen or cut follower holds the
  writer until Kafka drops it from the in-sync set, or until the thaw when both followers are
  frozen and no controller quorum is left to drop them. In the same rows with the defaults, the
  leader acknowledges alone, so the writer never waited longer than 0.1 seconds, apart from the 0.6
  seconds the driver takes to put a cut in place, and the reader waited instead, since it only gets
  a message once every in-sync copy has it. The cost is where an acknowledged message lives
  meanwhile: in `followers-pause` about 2,500 of them existed only on the leader for up to 25
  seconds, and a leader crash in that window would have lost them. That double fault wasn't run.
- **The idempotent producer can wait out its read timeout on a frozen broker.** After a timed-out
  batch it refreshes its metadata before it retries, from a broker it can't tell is stuck, and if
  that is the frozen follower it waits there until the thaw or `Net.ReadTimeout`, 30 seconds,
  whichever comes first. That happened in all three `followers-pause` runs, and in the one
  `follower-pause` run where Kafka took 10.9 seconds to drop the follower, just after the 10-second
  `Producer.Timeout`. See the metadata stall; reported as https://github.com/IBM/sarama/issues/3843.
  With the defaults no batch times out in these rows, so they never hit it.
- **With the defaults, a leader crash loses acknowledged messages.** All five live kills lost some,
  and none of the three with the idempotent settings did. Two runs lost one message, the last one
  the leader acknowledged before it died. Three lost every message written so far, 521 to 526: the
  new leader's first message got offset 0, so the follower that took over had copied nothing,
  about 0.6 seconds after the topic was created, and still counted as in sync, as every copy of a
  new partition does. Why it hadn't started copying isn't known. Each loss shows as a `lost-write`
  and an `inconsistent-offsets`, and each run has one `nonmonotonic-send` where the offsets go
  back. Kafka's own console consumer agrees with the final read of `defaults-2`: 11,762 records,
  the first at offset 0 with value 8239, and 12,288 acknowledged minus 526 lost is exactly 11,762.
  This is what Kafka documents for `acks=1`, not a sarama bug, but it is sarama's default: Kafka's
  Java producer has defaulted to acknowledgements from every in-sync copy, with idempotence, since
  Kafka 3.0 (KIP-679).
- **The harness hid it at first.** The defaults' `leader-kill` runs lost nothing, because the driver
  started every fault on its main loop and stopped writing while docker acted, so by the time the
  leader died, everything it had acknowledged had been copied. `leader-kill-live` starts the kill on
  a goroutine of its own and writes a value every millisecond (see the live kill).
- **Failed sends aren't losses.** In every kill run sarama gave up on the sends it made while there
  was no leader: 780 to 1,505 of 3,000 in `leader-kill`, and 6,758 to 11,205 of 20,000 in
  `leader-kill-live`. They are `info`, unknown, and in the runs checked none of them landed.

## kafka-go results

kafka-go's table ran on v0.4.51 against apache/kafka 4.3.1, each fault on a one-partition topic with
a solo helper reading, once with acknowledgements from every in-sync copy (`-acks all`), its
strongest setting, and once with its own default, no acknowledgement at all (`-acks none`). The runs
took place on 2026-10-23, at driver commits `a782401` (`leader-kill-live`) and `e4a9566` (the
freezes and the cut). A wait is the longest from handing a value over to its acknowledgement, for
the writer, or to its delivery, for the reader:

| Scenario | Every in-sync copy (`all`) | kafka-go's default (`none`) |
|---|---|---|
| `leader-kill-live` | clean ×3; the writer waited 9.4 to 14.9 s | clean ×3, since nothing was acknowledged; **200, 700 and 700 values reported sent were never stored** |
| `followers-pause` | **failed ×3**: one batch of 98 or 99 values stored 5 times; the writer waited 25.3 s | clean ×3; the writer never waited, the reader waited 25.3 s |
| `follower-pause` | **failed ×3**: one batch stored twice; the writer waited 9.0 to 9.7 s | clean ×3; the writer never waited, the reader waited 8.3 to 12.9 s |
| `follower-cut` | **failed ×3**: one batch stored 8 times; the writer waited 41.4 to 44.4 s | clean ×3; the writer never waited, the reader waited 37.6 to 43.7 s |

- **The settings check passed.** `leader-kill-live` also ran five times with the leader's
  acknowledgement alone (`-acks one`). One run lost acknowledged values, all 500 written so far: the
  follower that took over had copied nothing, about 0.6 seconds after the topic was created, as in
  three of sarama's five losses. One of the `all` runs met the same empty follower and lost nothing
  acknowledged: the leader can't acknowledge what only it holds, so it acknowledged nothing before
  the kill, and the writer waited 9.4 seconds for the new leader, which stored everything but the
  one batch that failed with an error. Same cluster state, opposite outcomes, as Kafka documents. In
  the run that lost them, the follower's broker log shows it had set up copying within 7
  milliseconds of the topic's creation, but never fetched the partition before the leader died; why
  isn't known. The other four runs lost nothing, and no run lost a batch acknowledged in the
  leader's last milliseconds, as sarama did twice, most likely because kafka-go sends one batch of
  about 100 values every 100 milliseconds here, so the leader rarely holds an acknowledged batch no
  follower has yet.
- **The strongest setting stores a batch again whenever a follower holds up the copy.** kafka-go has
  no idempotent producer. With `all` it asks the leader to wait at most 5 seconds, half its
  10-second write timeout, for every in-sync copy, and while a frozen or cut follower still counted
  as in sync, the leader answered REQUEST_TIMED_OUT and kafka-go sent the same batch again, which
  the leader, given no producer ID, stored again. Each run stored exactly one batch more than once,
  the first one sent after the fault began, once per attempt: 5 times through a 25-second freeze of
  both followers, twice before a frozen follower was fenced after 9.1 to 9.6 seconds, and 8 times, 8
  of kafka-go's 10 attempts, before a cut one was dropped after 41.1 to 44.5 seconds. The values
  behind it waited in kafka-go's queue and were stored once. Kafka's docs say a producer without
  idempotence may write duplicates when it retries; sarama's idempotent settings came through the
  same rows clean.
- **The default loses writes it reports as sent.** With `none`, the default of every `Writer` in
  kafka-go's README, kafka-go counts a batch as sent once it is written to the connection. In each
  leader crash two batches, 200 values, were reported sent and never stored, and in the two runs
  where the follower that took over had copied nothing, values 1 to 500 as well. The checker can't
  call them lost, since nothing promised them; the count comes from comparing what was handed over
  with the final read. Kafka's docs say a producer with `acks=0` won't generally know of any
  failures. Kafka's own Java producer has waited for every in-sync copy, with idempotence, by
  default since Kafka 3.0 (KIP-679).
- **Failed sends are few, and every one was reported.** In the crashes kafka-go gave up on 100 to
  200 sends whatever the setting, against sarama's 6,758 to 11,205: it retries only the batch at the
  head of the partition's queue, 10 times over about 7 seconds, while the values behind it wait.
  Each failed with "connection refused" from the dead leader's address, and none was stored.
- **The settings decide who waits, as with sarama.** With `all` the writer waited out each fault;
  with `none` it never waited, and the reader waited about as long instead.

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
- `relay` appears only on the meta line, set to `true` in a run that tests a program copying
  messages from one cluster to another. The acks then give positions in the source, and the final
  read gives positions in the copy (see Relay runs in v1 design).
- A consumer records `subscribe` (an `invoke`, then an `ok`) when it starts. Without it, a consumer
  that received nothing would leave no trace at all, and the checker would judge it clean.
- The consumer also records `disconnect` (an `info`, with the error) when its connection drops,
  and `reconnect` when it comes back. They show when the client noticed a network or server
  fault; the checker ignores them. With `-block-disconnect`, the disconnect handler then blocks
  for that long, the way a slow application handler would, and the meta line's params record it
  as `block_disconnect`. nats.go runs a connection's callbacks one at a time, so this shows
  whether the client's recovery waits behind them.
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
| `nonmonotonic-copy` | relay runs only: in one partition of the copy, the first copy of a value sits after the first copy of a value that the source stored later, in the same source partition | the program that copies |

In a Kafka history every rule applies within each partition, with offsets, which count from 0, in
place of stream sequences. A consumer has to reach the end of every partition the final read
returns, and a hole in any partition counts.
In a relay run (see Relay runs in v1 design), `nonmonotonic-copy` takes the place of
`inconsistent-offsets`, the source's acks can't explain a hole in the copy, and recovery and lag
aren't measured.

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
│   ├── franzgo/
│   │   ├── go.mod
│   │   ├── v1.20.6.mod
│   │   └── repro1505/
│   ├── kafkago/
│   │   └── go.mod
│   ├── relay/
│   │   └── go.mod
│   └── sarama/
│       ├── go.mod
│       ├── v1.61.0.mod
│       ├── pr3762-before.mod
│       ├── pr3762-after.mod
│       └── repro3843/
├── deploy/
│   ├── compose.yaml
│   ├── kafka.yaml
│   ├── kafka-source.yaml
│   └── nft/
└── scripts/
    ├── matrix.sh
    ├── kafka-calibrate.sh
    ├── sarama-calibrate.sh
    └── compare-nats.sh
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
| `drivers/franzgo/repro1505/` | a standalone reproducer for franz-go issue 1505, which needs no broker at all |
| `drivers/kafkago/` | a separate module that writes through kafka-go's Writer and, with a helper, reads through its Reader, with franz-go v1.20.7 as the witness |
| `drivers/kafkago/go.mod` | pins kafka-go v0.4.51 and franz-go v1.20.7 |
| `drivers/relay/` | a separate module that tests a program copying messages between two Kafka clusters: it starts the program with a given command and keeps it running as a supervisor would, writes into the source, can freeze or cut the copy's followers and crash the program meanwhile, and reads the copy back |
| `drivers/relay/go.mod` | pins franz-go v1.20.7 for the driver's own reads and writes, whatever the program under test uses |
| `drivers/sarama/` | a separate module that writes through sarama's idempotent producer, or with `-defaults` its default one, and, with a helper, reads through its partition consumer, with franz-go v1.20.7 as the witness; it can cut the partition's followers off from its leader, freeze them, or kill the leader |
| `drivers/sarama/go.mod` | pins sarama v1.61.1 and franz-go v1.20.7 |
| `drivers/sarama/v1.61.0.mod` | pins sarama v1.61.0, the last release without sarama PR 3762, used via `-modfile` |
| `drivers/sarama/pr3762-before.mod` | pins sarama at d0755db, the commit just before PR 3762, used via `-modfile` |
| `drivers/sarama/pr3762-after.mod` | pins sarama at 477992f, PR 3762's last commit, used via `-modfile` |
| `drivers/sarama/repro3843/` | a test that reproduces sarama issue 3843 with mock brokers, which needs no broker at all, and fails while the bug is there |
| `deploy/compose.yaml` | nats-server with JetStream (weekend 2), toxiproxy (weekend 3) |
| `deploy/kafka.yaml` | three Kafka 4.3.1 brokers for v1, in a compose project of their own |
| `deploy/kafka-source.yaml` | a one-broker Kafka cluster in a compose project of its own: the source for programs that copy messages between two clusters |
| `deploy/nft/` | the image the cut fault of the relay and sarama drivers runs `nft` from: Alpine with nftables, built once with `docker build -t driftlab-nft deploy/nft` |
| `scripts/matrix.sh` | runs the full matrix (2 versions × 2 APIs × 8 scenarios) and checks every verdict against the table |
| `scripts/kafka-calibrate.sh` | runs v1's calibration (2 franz-go versions × 2 scenarios × 3 repeats) and checks every verdict against the expected one |
| `scripts/sarama-calibrate.sh` | runs the sarama calibration (2 commits and 2 releases × 3 scenarios × 3 repeats), checks every verdict against the expected one, and counts each run's out-of-order rejections; `RUNS` and `ARGS` work as in `compare-nats.sh` |
| `scripts/compare-nats.sh` | runs the eight NATS scenarios against two or more driver builds, such as a nats.go pull request and the commit it starts from, and prints their verdicts, recovery and lag side by side; `RUNS` repeats them, `SCENARIOS` picks some, and `ARGS` adds driver flags such as `-block-disconnect` |

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
  `deploy/kafka.yaml`, explored by hand, and the decisions in v1 design. First exit reached on
  2026-10-14: the calibration pair reproduces, with franz-go v1.20.6 failing and v1.20.7 passing
  in all 12 runs (see v1 calibration results). The relay driver followed, and its first target,
  kaf-relay, gave two upstream reports, kaf-relay issues 48 and 49, and a third for franz-go
  itself, franz-go issue 1505 (see Relay results). sarama was calibrated on 2026-10-19, on the
  commits on either side of sarama PR 3762 (see sarama calibration results), and the freeze runs
  of its table gave a fourth upstream report, sarama issue 3843 (see the metadata stall). sarama's
  table was done on 2026-10-21, with the idempotent settings and with its defaults (see sarama
  results), and kafka-go's on 2026-10-23, with acknowledgements from every in-sync copy and with its
  default of none, after a settings check in place of a calibration (see kafka-go results).
  confluent-kafka-go comes next, and the version ends with the published table.

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
