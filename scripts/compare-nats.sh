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
#
# One run can be unlucky, so RUNS repeats every scenario, and SCENARIOS
# picks some of the eight:
#
#	RUNS=5 SCENARIOS="slow-long server-kill" scripts/compare-nats.sh legacy bin/a bin/b
#
# ARGS adds driver flags to every run, and its letters and digits to every
# file name, so those runs don't overwrite plain ones:
#
#	ARGS="-block-disconnect 30s" scripts/compare-nats.sh legacy bin/a bin/b
#
# Each scenario runs on every build back to back, so the rows to compare sit
# together and a busy moment on the machine lands on both builds.
set -u
cd "$(dirname "$0")/.." || exit 1

if [ $# -lt 2 ]; then
	echo "usage: [RUNS=n] [SCENARIOS=\"a b\"] [ARGS=\"-flag value\"] scripts/compare-nats.sh legacy|jetstream driver-binary..." >&2
	exit 2
fi
api=$1
shift

runs=${RUNS:-1}
if ! [[ $runs =~ ^[1-9][0-9]*$ ]]; then
	echo "RUNS must be a whole number above 0, not \"$runs\"" >&2
	exit 2
fi
read -r -a scenarios <<<"${SCENARIOS:-control proxy-control slow-short slow-long net-slow net-cut server-pause server-kill}"
read -r -a extra <<<"${ARGS:-}"
tag=$(printf '%s' "${ARGS:-}" | tr -cd '[:alnum:]')

out=runs/compare
mkdir -p "$out"

go build -o bin/driftlab ./cmd/driftlab || exit 1
docker compose -f deploy/compose.yaml up -d || exit 1
sleep 2 # give servers that compose just started a moment to begin listening

# driftlab check's exit code is a position in this list.
verdicts=(clean failed invalid "couldn't run")

if [ -n "$tag" ]; then
	echo "every run adds: $ARGS"
fi
printf '%-4s %-28s %-13s %-12s %-18s %s\n' run build scenario got recovery lag
for run in $(seq "$runs"); do
	for scenario in "${scenarios[@]}"; do
		for bin in "$@"; do
			build=$(basename "$bin")
			name=$build-$api-$scenario${tag:+-$tag}-$run
			rm -f "$out/$name.txt"
			# A race-detector build that finds a race exits non-zero, which
			# shows up here as a driver error. The ARGS flags go in through a
			# form that expands to nothing when there are none, because under
			# set -u, bash before 4.4 calls an empty array unbound.
			if "$bin" -api "$api" -scenario "$scenario" -grace 60s ${extra[@]+"${extra[@]}"} >"$out/$name.jsonl"; then
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

			printf '%-4s %-28s %-13s %-12s %-18s %s\n' "$run" "$build" "$scenario" "$got" "${recovery:--}" "${lag:--}"
		done
	done
done
