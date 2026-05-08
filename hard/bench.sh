#!/usr/bin/env bash
# Run ingest_hard N times in each mode, output sorted timing stats.
# Usage: ./bench.sh [folder] [mode] [N] [WARMUP]
#   folder default: ../XEUR-20260409-HJTR7RCAKT
#   mode   default: both (runs flat then hierarchy)
#   N      default: 20
#   WARMUP default: 5
set -eu
export LC_ALL=C

FOLDER="${1:-../XEUR-20260409-HJTR7RCAKT}"
MODES_REQUEST="${2:-both}"
N="${3:-20}"
WARMUP="${4:-5}"

BIN=./ingest_hard
[[ -x $BIN ]] || { echo "no $BIN — run make first" >&2; exit 1; }

if [[ $MODES_REQUEST == "both" ]]; then
    MODES=(flat hierarchy)
else
    MODES=("$MODES_REQUEST")
fi

# Warm cache: run flat several times, discard results.
echo "warming page cache ($WARMUP runs flat) ..." >&2
for _ in $(seq 1 "$WARMUP"); do
    PRINT_EVERY=100000000 $BIN "$FOLDER" flat >/dev/null 2>&1 || true
done

run_one() {
    local mode=$1
    local times=()
    for i in $(seq 1 "$N"); do
        local out
        out=$(PRINT_EVERY=100000000 $BIN "$FOLDER" "$mode" 2>&1)
        local t
        t=$(printf '%s' "$out" | grep '^Wall' | head -1 | awk '{print $3}' | tr -d 's')
        times+=("$t")
    done
    printf '%s\n' "${times[@]}" | sort -n | awk -v mode="$mode" -v n="$N" '
        { a[NR]=$1; sum+=$1 }
        END {
            min = a[1]; max = a[NR]
            med = (NR%2) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2
            p10 = a[int(NR*0.10)+1]
            p25 = a[int(NR*0.25)+1]
            p75 = a[int(NR*0.75)+1]
            p90 = a[int(NR*0.90)+1]
            avg = sum/NR
            printf "%-12s n=%2d  min=%5.3f  p10=%5.3f  p25=%5.3f  med=%5.3f  avg=%5.3f  p75=%5.3f  p90=%5.3f  max=%5.3f\n", \
                   mode, NR, min, p10, p25, med, avg, p75, p90, max
        }'
}

for m in "${MODES[@]}"; do
    run_one "$m"
done
