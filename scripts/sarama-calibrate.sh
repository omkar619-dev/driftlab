#!/usr/bin/env bash
# sarama-calibrate.sh runs driftlab's sarama calibration. Its pair is sarama
# at d0755db, the commit before PR 3762, and at 477992f, the pull request's
# last commit, which differ by that pull request alone. The releases on
# either side, v1.61.0 and v1.61.1, run too, because they are what people
# install, but 46 commits separate them, so they can't say which fix
# mattered. Every build goes through timeout-control and followers-cut. The
# script builds everything from the current commit, judges each run, and
# compares every verdict with the one DESIGN.md expects: only followers-cut
# on the two builds without PR 3762 should fail.
#
# Run it on the machine that hosts the Kafka brokers, because followers-cut
# changes their firewalls through docker:
#
#	scripts/sarama-calibrate.sh
#
# RUNS sets how many times each build runs each scenario (3). ARGS adds
# driver flags to every run, and its letters and digits to every file name,
# so those runs don't overwrite plain ones:
#
#	ARGS="-max-messages 1" scripts/sarama-calibrate.sh
#
# Each scenario runs on every build back to back, so the rows to compare sit
# together and a busy moment on the machine lands on every build. Each run's
# history and verdict land in runs/sarama-calibrate/. It exits 0 when every
# verdict is as expected, and 1 when any is not.
set -u
cd "$(dirname "$0")/.." || exit 1

runs=${RUNS:-3}
if ! [[ $runs =~ ^[1-9][0-9]*$ ]]; then
	echo "RUNS must be a whole number above 0, not \"$runs\"" >&2
	exit 2
fi
read -r -a extra <<<"${ARGS:-}"
tag=$(printf '%s' "${ARGS:-}" | tr -cd '[:alnum:]')

out=runs/sarama-calibrate
mkdir -p "$out"

# Each build is named after the sarama it pins. v1.61.1 is the module's own
# go.mod; every other build has a modfile of its own name.
builds=(pr3762-before pr3762-after v1.61.0 v1.61.1)
scenarios=(timeout-control followers-cut)

echo "building from commit $(git rev-parse --short HEAD)"
go build -o bin/driftlab ./cmd/driftlab || exit 1
for build in "${builds[@]}"; do
	modfile=()
	if [ "$build" != v1.61.1 ]; then
		modfile=(-modfile="$build.mod")
	fi
	go build -C drivers/sarama ${modfile[@]+"${modfile[@]}"} -o "../../bin/saramadriver-$build" . || exit 1
done
docker build -q -t driftlab-nft deploy/nft >/dev/null || exit 1
docker compose -f deploy/kafka.yaml up -d || exit 1
sleep 20 # give brokers that compose just started time to elect a controller

# expected prints the verdict the calibration expects for a build and a
# scenario. Only the cut breaks the builds without PR 3762.
expected() {
	case $1/$2 in
	pr3762-before/followers-cut | v1.61.0/followers-cut) echo failed ;;
	*) echo clean ;;
	esac
}

# driftlab check's exit code is a position in this list.
verdicts=(clean failed invalid "couldn't run")

if [ -n "$tag" ]; then
	echo "every run adds: $ARGS"
fi
total=0
misses=0
printf '%-4s %-16s %-14s %-8s %-12s %-10s %s\n' run scenario build expected got unknown anomalies
for run in $(seq "$runs"); do
	for scenario in "${scenarios[@]}"; do
		for build in "${builds[@]}"; do
			total=$((total + 1))
			name=$build-$scenario${tag:+-$tag}-$run
			rm -f "$out/$name.txt"
			if "bin/saramadriver-$build" -scenario "$scenario" ${extra[@]+"${extra[@]}"} >"$out/$name.jsonl"; then
				bin/driftlab check "$out/$name.jsonl" >"$out/$name.txt"
				got=${verdicts[$?]}
			else
				got="driver error"
			fi

			# How many publishes ended unknown, out of all of them, from the
			# verdict line, such as "1712/3200".
			unknown=$(head -1 "$out/$name.txt" 2>/dev/null | awk '{print $3 "/" $5}')

			# The anomaly kinds the checker reported, each with how many there
			# were, such as "inconsistent-offsets:2 lost-write:2".
			kinds=$(grep -v -E '^(verdict|invalid|recovery|lag)' "$out/$name.txt" 2>/dev/null | cut -d' ' -f1 | sort | uniq -c | awk '{printf "%s:%s ", $2, $1}')

			want=$(expected "$build" "$scenario")
			note=""
			if [ "$got" != "$want" ]; then
				note="  <- not as expected"
				misses=$((misses + 1))
			fi
			printf '%-4s %-16s %-14s %-8s %-12s %-10s %s%s\n' "$run" "$scenario" "$build" "$want" "$got" "${unknown:--}" "${kinds:--}" "$note"
		done
	done
done

if [ "$misses" -gt 0 ]; then
	echo "$misses of $total runs were not as expected; see $out/"
	exit 1
fi
echo "all $total runs as expected"
