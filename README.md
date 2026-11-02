# driftlab

driftlab tests whether streaming **client libraries** keep their documented delivery guarantees
under faults. Jepsen asks whether a server keeps its promises; driftlab asks whether the client
library you import keeps its promises.

It works in two halves:

1. **A driver** runs one scenario against a real broker and writes a *history*: a JSON line for
   every publish and its ack, every delivery, every fault it injected and every error the client
   reported.
2. **The checker**, `driftlab check`, reads that history afterwards and judges it. Every acked
   message must arrive, in order, with no skips and no duplicates, and the consumer must catch up
   on its own. It also measures how long each consumer took to recover, and how stale it got.

The history is the interface. The checker never touches the network, and an old history can be
judged again after a checker fix without re-running anything.

## Findings so far

Against nats.go, the Go client for NATS JetStream, at v1.53.1 and v1.54.0:

- **The push ordered consumer, and the KV watchers built on it, go silent for up to about 14
  seconds after reconnecting, with no error.** It happens after a disconnect longer than about 5
  seconds, or after any server restart, however short.
  https://github.com/nats-io/nats.go/issues/2180 (reported 2026-10-12). A maintainer has opened a
  fix, https://github.com/nats-io/nats.go/pull/2201, which driftlab checked; it isn't merged yet.
- **The `jetstream` ordered consumer uses a 15-second heartbeat instead of the 5 seconds it works
  out for itself, so it takes 30 seconds, not 10, to notice a silent line.**
  https://github.com/nats-io/nats.go/issues/2181 (reported 2026-10-12). Fixed by
  https://github.com/nats-io/nats.go/pull/2182, which driftlab checked too. It was merged on
  2026-10-19 and shipped in v1.55.0 on 2026-10-23.

Neither loses or duplicates a message.

Against franz-go, the Go client for Kafka:

- **Its default settings stored every message exactly once and in order, through every fault, on
  v1.22.2, its latest release, and on v1.20.7.** franz-go's defaults are already its strongest: an
  idempotent producer that waits for every in-sync copy. No send failed in 27 runs, and two leader
  crashes that came before a follower had copied anything lost nothing; with the other three
  clients, waiting for the leader alone, the same state lost every message written so far. On
  v1.20.7 a full buffer made `Produce` stop taking values for 3.4 seconds in two of three leader
  crashes, as franz-go documents; v1.22.1 raised the buffer from 10,000 records to 50,000, and on
  v1.22.2 the writer never paused.
- **A request doesn't return when its context ends while another request to the same broker is
  still dialing.** It waits for that dial to give up, 10 seconds by default. It surfaced in the
  kaf-relay runs below, where a call with a 1-second timeout took 10 seconds, and
  `drivers/franzgo/repro1505` reproduces it with no broker at all.
  https://github.com/twmb/franz-go/issues/1505 (reported 2026-10-17). The maintainer opened a fix
  the same day, https://github.com/twmb/franz-go/pull/1506, which driftlab checked: with it the
  call returns at its own deadline, and franz-go's fault table came through clean. It isn't merged
  yet.
- **After a leader crash, the reader found the new leader up to 5 seconds later than it could.**
  After a failed fetch, franz-go asks for fresh metadata, but drops the request if its last refresh
  finished less than 5 seconds before. Once the retry waits reach their 5-second cap, every second
  request lands a few milliseconds inside that limit and is dropped, so the reader refreshes every
  10 seconds instead of 5. With the defaults it came back 17 to 18 seconds after the kill in all
  twelve leader crashes, three of them during steady writing; with the limit lowered to 4.5 seconds
  it came back 2 to 3 seconds after the election, but a request was again dropped at the new line.
  https://github.com/twmb/franz-go/issues/1531 (reported 2026-10-31).

Against sarama, another Go client for Kafka, at v1.61.1:

- **The idempotent producer can wait out the whole 30-second read timeout on a broker that isn't
  answering, after the partition already takes writes again.** When a batch times out, sarama
  refreshes its metadata from a broker it picks before it retries, and with one request allowed
  per connection, every broker looks idle to it, a stuck one included. With one follower frozen
  for 60 seconds, it asked the frozen broker in 4 of 6 runs, and each of those resumed about 24
  seconds after the partition did, without losing a message. `drivers/sarama/repro3843`
  reproduces it with mock brokers. https://github.com/IBM/sarama/issues/3843 (reported
  2026-10-20). A maintainer suggested copying the Java client, which refreshes metadata before a
  retry only when the error says it is stale, and https://github.com/IBM/sarama/pull/3849 does
  that; it is waiting for review.
- **With its default settings, a leader crash loses acknowledged messages.** By default sarama
  waits only for the partition's leader to store a write, which Kafka documents can lose messages
  the leader acknowledged if it dies before a follower copies them; Kafka's own Java producer has
  waited for every in-sync copy by default since Kafka 3.0. In five leader crashes about 0.6
  seconds after the topic was created, every run lost acknowledged messages: one in two runs, and
  all 521 to 526 written so far in the other three, where the follower that took over hadn't
  copied anything yet. In three crashes during steady writing, one run lost the last two messages
  the leader acknowledged, 42 and 43 milliseconds after the kill began, and two lost nothing. With
  the idempotent settings, six crashes lost nothing. That is documented behavior, not a bug, and
  Kafka's own console consumer agrees with driftlab's count.

Against kafka-go, segmentio's Go client for Kafka, at v0.4.51:

- **With its default settings, a leader crash loses messages kafka-go reported as sent, with no
  error.** By default kafka-go's `Writer` asks for no acknowledgement at all, as every `Writer` in
  its README does, so it counts a batch as sent once it is written to the connection. In three
  leader crashes just after the topic was created, 200, 700 and 700 values were reported sent and
  never stored; in the two 700 runs, the follower that took over hadn't copied anything yet. In
  three during steady writing it was 300 every time, the first three batches sent after the crash
  began. Kafka documents this for `acks=0`: the producer won't generally know of any failures.
- **With its strongest settings, a follower that holds up the copy makes it store a batch again and
  again.** kafka-go has no idempotent producer. Waiting for every in-sync copy, it gives the leader
  5 seconds, then sends the batch again, and the leader stores it again: one batch was stored 5
  times while both followers were frozen for 25 seconds, twice before a frozen follower was
  dropped, and 8 times before a cut-off one was. Nothing acknowledged was lost. Kafka documents
  duplicates on retry for a producer without idempotence, and sarama's idempotent settings came
  through the same faults clean.

Both are documented behavior, not bugs. Kafka's own Java producer has waited for every in-sync
copy, with idempotence, by default since Kafka 3.0.

Against confluent-kafka-go, Confluent's Go client for Kafka, at v2.16.0 with librdkafka 2.16.0:

- **With its default settings, a slow follower or a leader crash can make it store a message
  twice.** By default librdkafka waits for every in-sync copy, so no crash lost an acknowledged
  message, but it isn't idempotent. A request the broker held past librdkafka's 30-second wait was
  sent again and stored again, out of order after messages sent since, in all three runs where a
  follower was cut off from the leader, and a batch of 5 or 6 values was stored twice in three of
  six leader crashes. librdkafka's docs say retrying may reorder messages unless idempotence is on.
- **Its idempotent producer stored every message exactly once and in order, through every fault.**
  In the cut, one message a run came back delivered but without an offset: its request had timed
  out, and librdkafka counted it as stored because the next request, which the broker could only
  accept after it, was acknowledged. It was stored, once, every time.

The first is documented behavior, not a bug.

The four Kafka clients side by side, each through the same five faults on the same three-broker
cluster. A cell is three runs, or five for sarama's defaults in the first crash:

| Client and settings | Leader crash just after the topic is created | Leader crash during steady writing | Both followers frozen | One follower frozen | One follower cut off |
|---|---|---|---|---|---|
| franz-go v1.22.2, its defaults | clean | clean | clean | clean | clean |
| sarama v1.61.1, idempotent | clean | clean | clean | clean | clean |
| sarama v1.61.1, its defaults | **lost acknowledged messages, 5 of 5** | **lost acknowledged messages, 1 of 3** | clean | clean | clean |
| kafka-go v0.4.51, `all` | clean | clean | **stored a batch again, 3 of 3** | **stored a batch again, 3 of 3** | **stored a batch again, 3 of 3** |
| kafka-go v0.4.51, its default `none` | **values sent, never stored, 3 of 3** | **values sent, never stored, 3 of 3** | clean | clean | clean |
| confluent-kafka-go v2.16.0, idempotent | clean | clean | clean | clean | clean |
| confluent-kafka-go v2.16.0, its defaults | **stored a batch twice, 1 of 3** | **stored a batch twice, 2 of 3** | clean | clean | **stored a message twice, 3 of 3** |

franz-go's defaults are already its strongest settings, so it has one row. Slow recovery isn't in
the table: sarama's idempotent producer can wait out its 30-second read timeout on a frozen broker,
and franz-go's reader came back after a leader crash up to 5 seconds later than it could, both
above.

Against kaf-relay, a program that keeps Kafka clusters in sync by copying topics, at v2.1.0
(`2dcedd3`):

- **A restart while the copy's cluster can't commit leaves a duplicate in the copy, and the next
  restart then skips a message, with no error.** It takes a crash and a restart while one
  in-sync follower in the copy's cluster has stopped replicating.
  https://github.com/zerodha/kaf-relay/issues/49 (reported 2026-10-17)
- **A relay doesn't start while the copy has caught up, and retries fetch errors with no
  backoff.** https://github.com/zerodha/kaf-relay/issues/48 (reported 2026-10-16)

Both are reported upstream and not yet confirmed by the maintainers.

Against Blnk, an open-source double-entry ledger for fintech products, at v0.15.7:

- **A transaction it answered `201 QUEUED` can be lost, with no error, when its API server stops.**
  Blnk's docs say it saves a queued transaction and then answers; in fact it answers first and
  saves in the background, so a crash or a restart loses whatever is still waiting, and queue
  recovery can't find it. On a 4-core test machine the saves fell behind the answers from 200
  transfers a second, and stopping or killing the server lost 246 transfers at 100 a second, 851
  at 400 and up to 6,011 at full speed, while every balance stayed consistent with the transfers
  that were applied. `scripts/blnk-repro-queued-loss.py` reproduces it.
  https://github.com/blnkfinance/blnk/issues/418 (reported 2026-11-02).
- **Under load, its balance updates deadlock with its own transaction inserts.** A
  `BEFORE UPDATE` trigger on the balances makes Postgres pick a row lock by the columns an update
  names, not the values it changes, and Blnk's update names `currency`, which is in a unique
  constraint. So each update takes the strongest row lock, the one the inserts' foreign-key checks
  wait for. Postgres broke 146 such deadlocks in the rate runs; when it aborted the insert, Blnk
  applied the transfer anyway, without its `QUEUED` record, 41 times at 800 a second.
  `scripts/blnk-pg-lockdemo.sh` shows the lock on plain Postgres 16.
  https://github.com/blnkfinance/blnk/issues/422 (reported 2026-11-02).
- **Its batching mostly redoes work that's already done.** A batch starts from one queued transfer
  and checks the others it adds, but not that one, which an earlier batch has often applied
  already; the batch then fails on its reference and is tried once more. At 800 transfers a
  second, 29,598 batches failed this way against 654 that worked. Nothing wrong was written.
  https://github.com/blnkfinance/blnk/issues/421 (reported 2026-11-02).

All three are reported upstream and not yet confirmed by the maintainers. These runs used Python
probes in `scripts/`, before a Blnk driver exists; the driver, the ledger checks and a calibration
come next.

Before any of that counted, driftlab was calibrated on a known, fixed bug,
https://github.com/nats-io/nats.go/issues/2107. It fails v1.53.1 exactly as the issue describes,
with 80 messages never delivered, and it passes v1.54.0, which has the fix. The Kafka side was
calibrated the same way, on https://github.com/twmb/franz-go/issues/1217: franz-go v1.20.6 fails
and v1.20.7 passes, and all 12 calibration runs came out as expected. sarama was calibrated on the
commits on either side of https://github.com/IBM/sarama/pull/3762: sending one message per request,
the commit before the fix lost two acknowledged messages in every run, and the one after it never
did. All 36 runs came out as expected. kafka-go has no idempotent producer to calibrate on, so it
was checked on what Kafka documents instead: waiting for the leader alone, a leader crash lost
acknowledged messages in one run of five, and waiting for every in-sync copy, in none of three.
confluent-kafka-go was checked the same way, as its last fixed data-loss bug, from 2022, can't be
triggered on this cluster: waiting for the leader alone, a crash lost acknowledged messages in
three runs of five, and its idempotent producer never did.

A fourth calibration, on 2026-10-22, against a regression that never shipped. The first commit on
https://github.com/nats-io/nats.go/pull/2201 recreates the push ordered consumer on reconnect and
loses the position it was asked to start at: a consumer that subscribed past a 10-message prefix
with `DeliverNew` was handed all 10 of them back after a single forced reconnect, in 5 runs out of
5. The maintainer's next commit on the same branch named and fixed it. The branch's head is clean,
and so are v1.53.1 and v1.54.0, 5 of 5 each, so the replay only ever existed between two commits of
an unmerged branch. It is recorded here for what it took to see it: the replay is ordered, gapless
and matches the stream, so every check driftlab had before it passed the run. It needed a new
scenario, `start-new`, and a new anomaly, `before-start`, which asks whether the consumer wanted
the messages at all rather than whether the sequence that arrived was consistent. All 25 runs came
out as expected. The first two commits are no longer reachable from any branch, so the builds were
`v1.54.1-0.20261022084953-e990cce972df`, which replays, and
`v1.54.1-0.20261022084953-06c31058b7f5`, which does not; the head tested was
`v1.54.1-0.20261022134643-fc1b9a9858c3`.

A fifth calibration, on 2026-10-23, against another regression that never shipped. The first commit
on https://github.com/nats-io/nats.go/pull/2201 recreates the push ordered consumer on reconnect,
and a disconnect that lands while that create is in flight unsubscribes it for good: the
subscription is gone until the process restarts, with no error raised to the caller. Driving 300
subscriptions through 10 forced reconnects 20ms apart killed 278, 296 and 297 of them in three
runs, each one reported with the sibling that carried on without it. The maintainer's next commit
on the same branch names the disconnect retries among its fixes, and the branch's head and the
release v1.54.0 are clean, 3 of 3 each. All 9 runs came out as expected. Seeing it at all needed a
new anomaly, `sub-death`: `checkStalls` already reports a consumer that ended behind the stream,
but behind is not dead, and a sibling that kept receiving past where this one stopped is what tells
the two apart. It also needed a subscription opened after the storm to be that sibling, because in
some runs every one of the 300 dies and nothing is left to compare against. The failing build was
`v1.54.1-0.20261022084953-e990cce972df` and the head tested was
`v1.54.1-0.20261022134643-fc1b9a9858c3`.

## The matrix

`scripts/matrix.sh` runs 2 nats.go versions × 2 consumer APIs × 8 scenarios: a control, a control
through the fault proxy, a stalled consumer (2 and 8 seconds), a slow line, a cut line, a frozen
server and a killed server. The latest run, on 2026-10-13 at commit `35fd84e`, came out exactly as
expected: 32 of 32. [DESIGN.md](https://github.com/omkar619-dev/driftlab/blob/main/DESIGN.md) has
the full results.

## Running it

You need Docker and Go 1.26 or newer, on one machine. The driver and the broker should share a
wired host, so the injected fault is the only thing that varies. The server scenarios run docker
themselves, so run the driver on the machine that hosts the containers.

```bash
docker compose -f deploy/compose.yaml up -d
go build -o bin/driftlab ./cmd/driftlab
go build -C drivers/natsgo -o ../../bin/natsdriver .
./bin/natsdriver -api legacy -scenario net-cut -grace 60s > cut.jsonl
./bin/driftlab check cut.jsonl
```

`driftlab check` exits 0 for clean, 1 for failed, 2 for invalid, and 3 when it couldn't run. A run
is invalid when it can't count as evidence, for example because a fault it declared never
happened.

The full matrix:

```bash
scripts/matrix.sh
```

The standalone reproducer for 2180 needs only a nats-server with JetStream:

```bash
go build -C drivers/natsgo -o ../../bin/repro2180 ./repro2180
./bin/repro2180
```

The one for franz-go 1505 needs no broker at all:

```bash
go build -C drivers/franzgo -o ../../bin/repro1505 ./repro1505
./bin/repro1505
```

The one for sarama 3843 is a test with mock brokers, and it fails while the bug is there:

```bash
go test -C drivers/sarama -run TestRetryWaitsForSilentBroker -count=1 -v ./repro3843
```

The one for Blnk 418 needs Python 3 and docker, with Blnk running from `deploy/blnk.yaml`:

```bash
docker compose -f deploy/blnk.yaml up -d
python3 scripts/blnk-repro-queued-loss.py --container blnk-server
```

The one for Blnk 422 needs only docker; it starts a throwaway Postgres 16 and removes it after:

```bash
bash scripts/blnk-pg-lockdemo.sh
```

## More

- [DESIGN.md](https://github.com/omkar619-dev/driftlab/blob/main/DESIGN.md): the principles, the
  history format, the checker's rules, every scenario and every result.
- v1, the Go Kafka clients, is done: franz-go and sarama are calibrated, kafka-go and
  confluent-kafka-go are checked, and the fault tables of all four are done, each with its
  strongest settings and with its defaults, which for franz-go are the same, and summed up in the
  table above. A relay driver tests programs that copy topics between clusters.
- v2, ledgers, has started with Blnk: probed so far, with a driver and ledger checks next.

Apache-2.0.
