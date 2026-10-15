#!/usr/bin/env bash
# compare-nats.sh runs the eight NATS scenarios against two or more builds
# of the NATS driver and prints each run's verdict, recovery and lag, so a
# nats.go change, such as a pull request, can be checked against the code it
# starts from:
#
#	scripts/compare-nats.sh legacy bin/natsdriver-main-dafa254 bin/natsdriver-pr2185-a08e662
#
# The first argument is the consumer API, legacy or jetstream; every later
# one is a driver binary built from this repo. There is no expected verdict:
# every scenario should come out clean on a correct build, and the point is
# the side-by-side numbers. Run it on the machine that hosts nats-server and
# toxiproxy. Each run's history and verdict land in runs/compare/.
set -u
cd "$(dirname "$0")/.." || exit 1

if [ $# -lt 2 ]; then
	echo "usage: scripts/compare-nats.sh legacy|jetstream driver-binary..." >&2
	exit 2
fi
api=$1
shift

out=runs/compare
mkdir -p "$out"

scenarios=(control proxy-control slow-short slow-long net-slow net-cut server-pause server-kill)

go build -o bin/driftlab ./cmd/driftlab || exit 1
docker compose -f deploy/compose.yaml up -d || exit 1
sleep 2 # give servers that compose just started a moment to begin listening

# driftlab check's exit code is a position in this list.
verdicts=(clean failed invalid "couldn't run")

printf '%-28s %-13s %-12s %-18s %s\n' build scenario got recovery lag
for bin in "$@"; do
	build=$(basename "$bin")
	for scenario in "${scenarios[@]}"; do
		name=$build-$api-$scenario
		rm -f "$out/$name.txt"
		# A race-detector build that finds a race exits non-zero, which
		# shows up here as a driver error.
		if "$bin" -api "$api" -scenario "$scenario" -grace 60s >"$out/$name.jsonl"; then
			bin/driftlab check "$out/$name.jsonl" >"$out/$name.txt"
			got=${verdicts[$?]}
		else
			got="driver error"
		fi

		# Keep the middle of "recovery consumer: caught up 13.006s after the
		# faults ended", and the duration from "lag consumer: the longest
		# wait from an ack to its delivery was 13.801s (value 3)". Runs
		# without faults have neither.
		line=$(grep '^recovery' "$out/$name.txt" 2>/dev/null)
		recovery=${line#*: }
		recovery=${recovery% after the faults ended}
		line=$(grep '^lag' "$out/$name.txt" 2>/dev/null)
		lag=${line#*was }
		lag=${lag%% *}

		printf '%-28s %-13s %-12s %-18s %s\n' "$build" "$scenario" "$got" "${recovery:--}" "${lag:--}"
	done
done
