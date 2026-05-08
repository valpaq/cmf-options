# HW1 — Data Ingestion Layer (C++)

*My day-to-day stack is Rust and Python; most of the C++ here was
typed by Claude Opus 4.7 under my direction — the design, architectural
decisions, and benchmarking strategy are mine, the code and plumbing
are the model's.*

C++20 ingestion layer for Eurex EUR/USD MBO NDJSON files. Two graded
tasks share the same parsing layer:

- Standard: parse one daily file, emit `MarketDataEvent` to a
  consumer, summarize.
- Hard: parse a folder of ~20 daily files in parallel, merge into one
  chronological stream via two strategies (flat k-way / hierarchy
  tree), benchmark and cross-check.

Full task text: [`docs/assignment.md`](docs/assignment.md).

## Result

Apple M3 Pro, 12 cores, 18 GB. 22 daily Eurex MBO files
(`XEUR-20260409-HJTR7RCAKT`, 4.9 GB on disk, 15.18 M kept events).
Numbers are best/median of 20 runs after a 6-run warmup; load average
during measurement was 3–9.

### Standard task — single 263 MB file, ~796 K events

| Variant | Wall (best) | Throughput |
|---|---|---|
| `ingest_opt` (simdjson reference) | 0.108 s | ~7.4 M evt/s |
| `ingest_fast` (custom schema-aware) | 0.042 s | ~19 M evt/s |

### Hard task — full 22-file folder, 15.18 M events, 4.9 GB

Best/median across two bench rounds (each n=20 after 6-run warmup).
The wide range is host load: load avg 3–6 hits the lower bound, load
avg 6–10 hits the upper bound. On an idle machine the binary lands
near the lower bound run after run.

| Strategy | Wall (best) | Wall (median) | Throughput (best) |
|---|---|---|---|
| Flat (k-way merge)  | 1.80 – 2.32 s | 2.27 – 2.69 s | 6.5 – 8.4 M evt/s |
| Hierarchy (4-way fan-in) | 1.30 – 1.66 s | 1.72 – 2.34 s | 9.1 – 11.7 M evt/s |

End-to-end on hier-best (1.30 s): 11.7 M evt/s parsed + merged +
cross-check hashed, throughput ~3.8 GB/s through the full pipeline.
Hierarchy is 25–30 % ahead of flat on best, ~24 % on median. Flat
and hierarchy emit byte-identical streams (FNV-1a hash
`0x5d877913a896e16d` matches when `mode == both`).

## Layout

```
viktor_paramonov_hw1/
├── README.md                ← this file
├── Makefile                 ← top-level: make / make standard / make hard
├── docs/
│   ├── assignment.md        ← combined Standard + Hard spec
│   ├── documentation.md     ← Databento format reference
│   ├── perf_journey.md      ← what we tried, what worked, what didn't
│   └── next-steps.md        ← what's next (Hard merger, LOB, backtester)
├── common/                  ← shared parser primitives (used by both binaries)
│   ├── event.h              ← canonical 64-byte MarketDataEvent + enums + flags
│   ├── parse.h              ← SWAR helpers, civil-calendar math, mmap loader, summary
│   └── fast_parse.h         ← schema-aware NDJSON parser (no simdjson dependency)
├── standard/
│   ├── README.md
│   ├── Makefile
│   └── src/
│       ├── main.cpp         ← ingest_opt   (simdjson reference variant)
│       ├── main_fast.cpp    ← ingest_fast  (custom-parser variant)
│       └── parse_simdjson.h ← simdjson `parse_event` (used only by ingest_opt)
├── hard/
│   ├── README.md
│   ├── Makefile
│   ├── bench.sh             ← timing harness (warmup + N runs, prints percentiles)
│   └── src/
│       ├── main_hard.cpp    ← producers + flat / hierarchy mergers + dispatcher
│       ├── chunk.h          ← line-aligned splitter (mmap → ChunkSpec list)
│       ├── spsc_ring.h      ← batched single-producer / single-consumer ring
│       └── tournament.h     ← templated winner-tree k-way merge
└── tests/
    ├── Makefile
    ├── test_main.cpp        ← unit tests for parser + chunk splitter + ring
    └── smoke.sh             ← black-box smoke (empty file / empty folder)
```

## Build

Dependencies:
- C++20 compiler with `<format>` (clang ≥ 16 / Apple clang 15+ / gcc ≥ 13)
- POSIX `mmap` / `fcntl` (Linux or macOS)
- pthreads (Hard binary only)
- `simdjson` for the Standard `ingest_opt` only — auto-detected via
  `pkg-config`, falls back to `-lsimdjson`. The Hard binary and
  Standard `ingest_fast` do not depend on simdjson.

```bash
make            # builds standard/ingest_opt + standard/ingest_fast + hard/ingest_hard
make standard   # just standard
make hard       # just hard
make rebuild    # clean + all
make test       # unit tests (parser + splitter + ring)
make smoke      # black-box smoke (empty file / empty folder)
```

Compile flags: `-std=c++20 -O3 -march=native -flto -DNDEBUG -pthread`.

## Run

### Standard

```bash
cd standard
./ingest_fast /path/to/2026-04-01_xeur-eobi.mbo.json    # custom parser (recommended)
./ingest_opt  /path/to/2026-04-01_xeur-eobi.mbo.json    # simdjson reference
# or
make run      FILE=/path/to/file.ndjson
make run-fast FILE=/path/to/file.ndjson
```

Output: first 10 and last 10 parsed `MarketDataEvent` records, then a
summary (total messages, parse errors, dropped events, fallback
timestamps, flag counters, first/last timestamp, wall time, throughput).

### Hard

```bash
cd hard
./ingest_hard /path/to/folder/with/22/ndjson [flat|hierarchy|both]
# or
make run FOLDER=/path/to/dir [MODE=flat|hierarchy|both]
# benchmark harness — warmup + N runs, percentiles
./bench.sh /path/to/folder both 20 6
```

`PRINT_EVERY=N` controls how often `processMarketDataEvent` prints a
sample (Hard default `1000000`; Standard default `100000`;
`PRINT_EVERY=1` prints every event). `CHUNK_MB=N` sets the
chunk-parallel parsing size for the Hard binary (default 192 MiB; set
very high to disable).

When `mode == both` the binary runs flat then hierarchy and compares
their FNV-1a event hashes. Mismatch exits with code 2 (used by CI).

## What we built — short version

The shared parser (`common/fast_parse.h`) is schema-aware: every
Databento MBO line has the same key order, so the hot path
(`parse_line_positional`) bulk-`memcmp`s constant `,"key":` prefixes
and reads each value at the known offset. Lines that don't match the
prefix fall through to `parse_line_generic` (key-dispatch fallback).
The Standard `ingest_fast` and the Hard binary share this code path
verbatim.

The Hard pipeline decouples producer and merger via deep SPSC rings:

```
       ┌─ chunk-parallel producers ─┐    deep BatchedSpscRing
22 files mmap'd, line-aligned chunks  →  (8192 evt/batch × 64 slots
       └────────────────────────────┘    ≈ 524 K evt buffered/edge)
                          │
              ┌───────────┴───────────┐
              │ Flat: one k-way       │   single-thread merge consumer,
              │ tournament tree on    │   FNV-1a hash + toy LOB,
              │ ~30 chunk leaves      │   processMarketDataEvent
              └───────────────────────┘
                          OR
              ┌───────────┴───────────┐
              │ Hierarchy: 4-way      │   ~11 merger threads,
              │ tournament-tree mergers│  log4(N) ≈ 3 levels deep,
              │ collapsing ~30 leaves │   QoS-pinned to P-cluster
              └───────────────────────┘
```

The producer side is parallel; the merger side is single-thread per
strategy by design — a globally-ordered merge can't be parallelized
naively without re-ordering. 11.7 M evt/s end-to-end means the
producer→ring→merger→consumer chain isn't bound by parse: producer
threads are mostly idle waiting on full rings.

For the optimization story see [`docs/perf_journey.md`](docs/perf_journey.md).

## Cross-check

Both Hard strategies independently FNV-1a hash every dispatched event
across (key_ts_ns, ts_recv_ns, price, size, sequence, order_id,
instrument_id, flags, action, side, has_price). Matching hashes prove
the two strategies emit byte-identical streams in identical order.
The toy LOB also reports `Distinct instruments` and
`LOB best-bid/ask updates` as a secondary cross-check.

```
=== Cross-check (flat vs hierarchy) ===
OK — flat and hierarchy emit byte-identical streams.
```

## Data-quality counters

Per-run summary reports flags called out in `docs/documentation.md`:

| Counter | Meaning |
|---|---|
| `Dropped (no usable ts)` | Events with neither `ts_recv` nor `ts_event`; excluded from merge |
| `Used ts_event fallback` | Events that fell back to `ts_event` because `ts_recv` was missing |
| `F_BAD_TS_RECV` | Bit 3 (`0x8`): `ts_recv` is inaccurate (clock issues / packet reordering) |
| `F_MAYBE_BAD_BOOK` | Bit 2 (`0x4`): unrecoverable channel gap detected |

In raw fixed-precision price strings, the literal `"9223372036854775807"`
(`UNDEF_PRICE`, `INT64_MAX`) is treated as «no price», same as JSON `null`.

## Deliverables map

| Required | File |
|---|---|
| `MarketDataEvent` class | [`common/event.h`](common/event.h) |
| Standard task source | [`standard/src/main.cpp`](standard/src/main.cpp), [`standard/src/main_fast.cpp`](standard/src/main_fast.cpp) |
| Hard task source | [`hard/src/main_hard.cpp`](hard/src/main_hard.cpp) |
| Flat merger | [`hard/src/main_hard.cpp::run_flat`](hard/src/main_hard.cpp), [`hard/src/tournament.h`](hard/src/tournament.h) |
| Hierarchy merger | [`hard/src/main_hard.cpp::run_hierarchy`](hard/src/main_hard.cpp), [`hard/src/main_hard.cpp::merger_node_main`](hard/src/main_hard.cpp), [`hard/src/tournament.h`](hard/src/tournament.h) |
| Producer threads | [`hard/src/main_hard.cpp::producer_main`](hard/src/main_hard.cpp), [`hard/src/spsc_ring.h`](hard/src/spsc_ring.h) |
| Performance report | this README + [`docs/perf_journey.md`](docs/perf_journey.md) |
| Benchmark harness | [`hard/bench.sh`](hard/bench.sh) |
| Build instructions | this README + Makefiles |
