#!/usr/bin/env bash
# Black-box smoke for input edge cases that the C++ unit tests don't reach:
# behaviour of the binaries on an empty file and an empty folder.
#
# Assumes the binaries have been built (make standard / make hard).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INGEST_FAST="$ROOT/standard/ingest_fast"
INGEST_OPT="$ROOT/standard/ingest_opt"
INGEST_HARD="$ROOT/hard/ingest_hard"

for bin in "$INGEST_FAST" "$INGEST_OPT" "$INGEST_HARD"; do
    if [[ ! -x "$bin" ]]; then
        echo "missing binary: $bin (run 'make' from repo root)" >&2
        exit 1
    fi
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

empty_file="$TMP/empty.mbo.json"
: > "$empty_file"
empty_dir="$TMP/empty_dir"
mkdir -p "$empty_dir"

fail=0

check_empty_file_binary() {
    local label="$1" bin="$2"
    local out
    if ! out="$("$bin" "$empty_file" 2>&1)"; then
        echo "FAIL $label on empty file: non-zero exit"
        echo "$out"
        fail=$((fail + 1))
        return
    fi
    if ! grep -q "Total messages processed: 0" <<<"$out"; then
        echo "FAIL $label on empty file: missing 'Total messages processed: 0'"
        echo "$out"
        fail=$((fail + 1))
        return
    fi
    echo "ok   $label on empty file"
}

check_empty_file_binary "ingest_fast" "$INGEST_FAST"
check_empty_file_binary "ingest_opt"  "$INGEST_OPT"

# ingest_hard takes a folder. Empty folder must exit non-zero with a clear
# message; we don't want it crashing or silently producing 0-byte output.
if out="$("$INGEST_HARD" "$empty_dir" 2>&1)"; then
    echo "FAIL ingest_hard on empty folder: expected non-zero exit"
    echo "$out"
    fail=$((fail + 1))
else
    if ! grep -qi "no .* files found" <<<"$out"; then
        echo "FAIL ingest_hard on empty folder: missing 'No ... files found' message"
        echo "$out"
        fail=$((fail + 1))
    else
        echo "ok   ingest_hard on empty folder"
    fi
fi

# Empty folder for the hard binary, but in 'flat' mode, must also fail
# cleanly (folder check happens before mode dispatch).
if "$INGEST_HARD" "$empty_dir" flat >/dev/null 2>&1; then
    echo "FAIL ingest_hard flat on empty folder: expected non-zero exit"
    fail=$((fail + 1))
else
    echo "ok   ingest_hard flat on empty folder"
fi

if [[ "$fail" -eq 0 ]]; then
    echo "smoke: all checks passed"
    exit 0
fi
echo "smoke: $fail check(s) failed" >&2
exit 1
