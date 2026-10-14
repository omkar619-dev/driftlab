#!/usr/bin/env bash
# kafka-calibrate.sh runs driftlab's v1 calibration: franz-go v1.20.6, the
# last release with issue 1217, and v1.20.7, the fix, each through the
# deadline-control and followers-pause scenarios, several times over. It
# builds everything from the current commit, judges each run, and compares
# every verdict with the one DESIGN.md expects: only followers-pause on
# v1.20.6 should fail.
#
# Run it on the machine that hosts the Kafka brokers, because the
# followers-pause scenario freezes them with docker:
#
#	scripts/kafka-calibrate.sh
#
# Each run's history and verdict land in runs/kafka-calibrate/. It exits 0
# when every verdict is as expected, and 1 when any is not.
set -u
cd "$(dirname "$0")/.." || exit 1

out=runs/kafka-calibrate
mkdir -p "$out"

versions=(v1.20.6 v1.20.7)
scenarios=(deadline-control followers-pause)
repeats=3

echo "building from commit $(git rev-parse --short HEAD)"
go build -o bin/driftlab ./cmd/driftlab || exit 1
go build -C drivers/franzgo -o ../../bin/franzdriver-v1.20.7 . || exit 1
go build -C drivers/franzgo -modfile=v1.20.6.mod -o ../../bin/franzdriver-v1.20.6 . || exit 1
docker compose -f deploy/kafka.yaml up -d || exit 1
sleep 20 # give brokers that compose just started time to elect a controller

# expected prints the verdict the calibration expects for a version and a
# scenario. Only the freeze breaks the buggy release.
expected() {
	if [ "$1" = v1.20.6 ] && [ "$2" = followers-pause ]; then
		echo failed
	else
		echo clean
	fi
}

# driftlab check's exit code is a position in this list.
verdicts=(clean failed invalid "couldn't run")

runs=0
misses=0
printf '%-8s %-17s %-4s %-8s %-12s %s\n' version scenario run expected got anomalies
for version in "${versions[@]}"; do
	for scenario in "${scenarios[@]}"; do
		for i in $(seq "$repeats"); do
			runs=$((runs + 1))
			name=$version-$scenario-$i
			rm -f "$out/$name.txt"
			if "bin/franzdriver-$version" -scenario "$scenario" >"$out/$name.jsonl"; then
				bin/driftlab check "$out/$name.jsonl" >"$out/$name.txt"
				got=${verdicts[$?]}
			else
				got="driver error"
			fi

			# The anomaly kinds the checker reported, in order, such as
			# "phantom lost-write inconsistent-offsets".
			kinds=$(grep -v -E '^(verdict|invalid|recovery|lag)' "$out/$name.txt" 2>/dev/null | cut -d' ' -f1 | tr '\n' ' ')

			want=$(expected "$version" "$scenario")
			note=""
			if [ "$got" != "$want" ]; then
				note="  <- not as expected"
				misses=$((misses + 1))
			fi
			printf '%-8s %-17s %-4s %-8s %-12s %s%s\n' "$version" "$scenario" "$i" "$want" "$got" "${kinds:--}" "$note"
		done
	done
done

if [ "$misses" -gt 0 ]; then
	echo "$misses of $runs runs were not as expected; see $out/"
	exit 1
fi
echo "all $runs runs as expected"
