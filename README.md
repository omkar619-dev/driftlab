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
  https://github.com/nats-io/nats.go/issues/2181 (reported 2026-10-12). A fix is under review,
  https://github.com/nats-io/nats.go/pull/2182, which driftlab checked too.

Neither loses or duplicates a message.

Against kaf-relay, a program that keeps Kafka clusters in sync by copying topics, at v2.1.0
(`2dcedd3`):

- **A restart while the copy's cluster can't commit leaves a duplicate in the copy, and the next
  restart then skips a message, with no error.** It takes a crash and a restart while one
  in-sync follower in the copy's cluster has stopped replicating.
  https://github.com/zerodha/kaf-relay/issues/49 (reported 2026-10-17)
- **A relay doesn't start while the copy has caught up, and retries fetch errors with no
  backoff.** https://github.com/zerodha/kaf-relay/issues/48 (reported 2026-10-16)

Both are reported upstream and not yet confirmed by the maintainers.

Before any of that counted, driftlab was calibrated on a known, fixed bug,
https://github.com/nats-io/nats.go/issues/2107. It fails v1.53.1 exactly as the issue describes,
with 80 messages never delivered, and it passes v1.54.0, which has the fix. The Kafka side was
calibrated the same way, on https://github.com/twmb/franz-go/issues/1217: franz-go v1.20.6 fails
and v1.20.7 passes, and all 12 calibration runs came out as expected.

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

## More

- [DESIGN.md](https://github.com/omkar619-dev/driftlab/blob/main/DESIGN.md): the principles, the
  history format, the checker's rules, every scenario and every result.
- v1, the Go Kafka clients, is under way: franz-go is calibrated, and a relay driver tests
  programs that copy topics between clusters. sarama, kafka-go and confluent-kafka-go come next.

Apache-2.0.
