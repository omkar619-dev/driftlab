# driftlab: design (v0)

> Status: draft, last updated 2026-09-26. v0 = the checker + one system (NATS JetStream, Go client).

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
nonmonotonic-poll, duplicate) comes from Jepsen's Kafka workload
(https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html). We reuse those names on purpose, so
anyone who knows Jepsen can read driftlab output without a glossary.

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

**Faults in v0 are client-side only.** The consumer's callback stalls while a low pending limit is
set, which is exactly the 2107 trigger. Network and server faults come in weekend 3.

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
| `slow-short` | 2s, shorter than the 5s ordered-consumer heartbeat | the path in the issue: no reset ever fires |
| `slow-long` | 8s, longer than the heartbeat | the heartbeat reset fires, but it resumes from a sequence that has already been advanced past messages that were never delivered |

`slow-long` matters because a fix that only covers the short path passes `slow-short` and still
fails here.

A stall scenario declares two faults. `stall` is the blocked callback. `overflow` is what the stall
is meant to cause: the client's pending tray fills up and nats.go drops messages, which it reports
as a slow consumer error. The driver marks `overflow` as done only when that error arrives, so a
run whose tray never overflowed is invalid rather than clean, because the 2107 trigger never fired.
(The `jetstream` API has no drop path, so its stall scenarios will declare only `stall`.)

## Calibration matrix (the v0 exit criterion)

| Scenario | legacy @ v1.53.1 | legacy @ v1.54.0 | jetstream @ v1.53.1 | jetstream @ v1.54.0 |
|---|---|---|---|---|
| control | clean | clean | clean | clean |
| slow-short | **poll-skip** (the issue's repro loses 80 of 100) | clean | clean | clean |
| slow-long | **poll-skip** expected | clean expected | clean | clean |

The expected values come from the issue and the fix, and driftlab must reproduce them from
black-box observation alone. If `legacy @ v1.54.0` fails `slow-long`, that is a real finding. It
goes to the nats.go maintainers with the history file attached.

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
  value is `meta`. v0 records every publish error as `info`. Telling which errors are definite
  failures takes care, so that comes later.
- `value` is unique per message across the run: the producer assigns 1, 2, 3, and so on. This
  makes every message identifiable end to end, independently of the server.
- `stream_seq` is the server's sequence. It appears on `ok` publishes (taken from the PubAck), on
  every delivery (taken from the message metadata) and on final-read records.
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
- `time_ns` is monotonic time since the driver started, not wall-clock time. That follows from
  principle 4. Once one run has several processes, the harness will stamp receive times instead.
- Unknown fields are ignored, so the format can grow.

**Final read:** after the scenario ends, the driver reads every stream sequence back by number,
without a consumer, so the ground truth doesn't depend on the machinery under test. That read is
the ground truth for what the stream contains.

## Checker (v0)

The checker is a pure function, `Check(history) -> Result`, with no network access and no clocks.
Even liveness is judged by position in the history (principle 4).

| Anomaly | Definition | Usually whose bug |
|---|---|---|
| `lost-write` | an `ok` publish whose value is missing from the final read | server (durability) |
| `poll-skip` | the consumer jumps from stream_seq a to some b > a+1; the skipped range is then classified as *delivered late* or *never delivered* | client |
| `nonmonotonic-poll` | the consumer delivers b after a, with b < a | client |
| `duplicate` | one consumer receives the same stream_seq twice | client |
| `stall` | when the final read begins, the consumer hasn't yet received the stream's last sequence. Consumers are known from their deliveries and their `subscribe` operations, so one that received nothing is still judged | client (liveness) |
| `phantom` | a value seen by the consumer or the final read that no producer ever invoked, or whose publish was recorded as `fail` | client or server |

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
nothing, but with a 20-message tray it recovered one tray per 5-second heartbeat, because each
refetch overflowed the tray again. A 2-second stall cost about 15 seconds of recovery, and the
run's clean verdict cleared the grace window by milliseconds.

Each anomaly carries the history indexes of the ops involved. It also carries any `client-error`
and nemesis events that fall inside the window, so the output points at evidence instead of just
summarising it.

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

`driftlab check <history.jsonl>` prints the verdict, the invalid reasons and the anomalies, one per
line. The exit code is the verdict for scripts: 0 clean, 1 failed, 2 invalid, and 3 when the
command couldn't run (bad usage, or a file it can't open or parse). An invalid run exits non-zero
on purpose, so a script can never mistake a broken experiment for a pass.

## Repo layout (v0)

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
│   └── natsgo/
│       ├── go.mod
│       └── v1.53.1.mod
└── deploy/
    └── compose.yaml
```

| Path | What it is |
|---|---|
| `go.mod` | the root module: the checker and the CLI, with no client libraries |
| `cmd/driftlab/` | the CLI: `driftlab check <history.jsonl>` |
| `internal/history/` | the Op type and the JSONL reader |
| `internal/checker/` | `Check()` and its anomaly kinds |
| `internal/checker/testdata/` | hand-written histories: one clean, one per anomaly kind |
| `drivers/natsgo/` | a separate module that talks to NATS and writes a history to stdout |
| `drivers/natsgo/go.mod` | pins nats.go v1.54.0 (the fixed release) |
| `drivers/natsgo/v1.53.1.mod` | pins nats.go v1.53.1 (the last buggy release), used via `-modfile` |
| `deploy/compose.yaml` | nats-server with JetStream (weekend 2), toxiproxy (weekend 3) |

## Milestones

- **2026-09-25 (done):** the repo, this document, go.mod and .gitattributes, pushed.
- **Weekend 1 (done 2026-09-27; the checker, no NATS at all):** the history reader, `Check`, the verdict rules, and
  table-driven tests over hand-written histories. The fixtures are one clean history, one per anomaly kind, plus
  `info` publishes that are present and absent (both must pass). Exit: `driftlab check` flags
  every bad fixture and passes every good one. That is calibration step (a).
- **Weekend 2 (the driver and calibration):** the natsgo driver (both APIs, both versions), the
  compose file and the matrix above. Exit: the matrix reproduces. That is calibration steps (b)
  and (c).
- **Weekend 3 (network and server faults):** toxiproxy between the driver and nats-server, for
  connection cuts and latency above the heartbeat interval. Docker pause, kill and restart of
  nats-server. Run the same matrix again. This is the first point where driftlab can find
  something nobody has reported yet.

## Beyond v0 (one line each, so v0 stays small)

- **v1:** the Go Kafka client matrix (franz-go, sarama, kafka-go, confluent-kafka-go) against one
  broker, with consumer-group rebalances and leader elections. Then Redpanda and Redis Streams.
  The output is a published table of client × documented guarantee × verdict under each fault.
- **v2:** backpressure, exactly-once claims, redelivery semantics, time-to-recovery.
- **v3:** seeded fault schedules, shrinking a failing schedule down to a minimal one, and a CI mode
  so maintainers can run it nightly.
- **v4:** each client's documented guarantees encoded as machine-readable claims files.

## Open questions (decided when we reach them)

- **Batch deliveries.** Kafka consumers poll in batches, while push callbacks deliver one message
  at a time. Should `deliver` events grow a batch form, the way Jepsen models polls? Kafka offsets
  also start at 0, which breaks the history package's convention that 0 means "absent".
  *Comes up:* in v1, when the first Kafka driver is written.
- **Orchestration.** How much of a `driftlab run` command do we need? *Comes up:* at the end of
  weekend 2. The matrix is 12 runs (3 scenarios × 2 APIs × 2 versions). That's fine by hand once,
  and worth a script by the second time.
- **Durable consumers.** With acks, duplicates become legal, but only when they're marked as
  redeliveries. That's a different contract, so it probably needs a separate checker mode.
  *Comes up:* in v2, with redelivery semantics.
