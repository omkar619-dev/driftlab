#!/usr/bin/env bash
# matrix.sh runs driftlab's calibration matrix: every combination of the
# nats.go versions, consumer APIs and scenarios listed below. It builds
# everything from the current commit, judges each run, and compares every
# verdict with the one DESIGN.md's calibration matrix expects. Only the
# legacy API on v1.53.1, the last release with the 2107 bug, should fail,
# and only when stalled.
#
# Run it on the machine that hosts nats-server and toxiproxy:
#
#	scripts/matrix.sh
#
# Each run's history and verdict land in runs/matrix/. It exits 0 when every
# verdict is as expected, and 1 when any is not.
set -u
cd "$(dirname "$0")/.." || exit 1

out=runs/matrix
mkdir -p "$out"

versions=(v1.53.1 v1.54.0)
apis=(legacy jetstream)
scenarios=(control proxy-control slow-short slow-long)

# The fixed legacy consumer takes 10 to 18 seconds to catch up after a stall
# with a 20-message tray (DESIGN.md, Recovery). Under the default 10-second
# grace window its clean verdict once passed by milliseconds, so the matrix
# gives it more room. A consumer that catches up early ends the wait early;
# only a stuck one waits out the whole window.
grace=60s

echo "building from commit $(git rev-parse --short HEAD)"
go build -o bin/driftlab ./cmd/driftlab || exit 1
go build -C drivers/natsgo -o ../../bin/natsdriver-v1.54.0 . || exit 1
go build -C drivers/natsgo -modfile=v1.53.1.mod -o ../../bin/natsdriver-v1.53.1 . || exit 1
docker compose -f deploy/compose.yaml up -d || exit 1
sleep 2 # give servers that compose just started a moment to begin listening

# expected prints the verdict the calibration matrix expects for a version,
# an API and a scenario. Only the stall scenarios break the buggy release.
expected() {
	if [ "$1" = v1.53.1 ] && [ "$2" = legacy ] && [[ $3 == slow-* ]]; then
		echo failed
	else
		echo clean
	fi
}

# driftlab check's exit code is a position in this list.
verdicts=(clean failed invalid "couldn't run")

runs=0
misses=0
printf '%-8s %-9s %-13s %-8s %-12s %s\n' version api scenario expected got recovery
for version in "${versions[@]}"; do
	for api in "${apis[@]}"; do
		for scenario in "${scenarios[@]}"; do
			runs=$((runs + 1))
			name=$version-$api-$scenario
			rm -f "$out/$name.txt"
			if "bin/natsdriver-$version" -api "$api" -scenario "$scenario" -grace "$grace" >"$out/$name.jsonl"; then
				bin/driftlab check "$out/$name.jsonl" >"$out/$name.txt"
				got=${verdicts[$?]}
			else
				got="driver error"
			fi

			# Keep the middle of "recovery consumer: caught up 13.006s after
			# the faults ended". A run without faults has no recovery line.
			line=$(grep '^recovery' "$out/$name.txt" 2>/dev/null)
			recovery=${line#*: }
			recovery=${recovery% after the faults ended}

			want=$(expected "$version" "$api" "$scenario")
			note=""
			if [ "$got" != "$want" ]; then
				note="  <- not as expected"
				misses=$((misses + 1))
			fi
			printf '%-8s %-9s %-13s %-8s %-12s %s%s\n' "$version" "$api" "$scenario" "$want" "$got" "${recovery:--}" "$note"
		done
	done
done

if [ "$misses" -gt 0 ]; then
	echo "$misses of $runs runs were not as expected; see $out/"
	exit 1
fi
echo "all $runs runs as expected"
