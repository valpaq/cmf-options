# Hard task — multi-file producer + flat / hierarchy mergers

Reads ~20 daily Eurex EOBI MBO NDJSON files in parallel, merges them
into one global chronological stream, and feeds events to
`processMarketDataEvent`. Implements both required merge strategies:

1. Flat: single k-way tournament-tree merge over all sources.
2. Hierarchy: 4-way tree of mergers; collapses ~30 chunk leaves in
   ~3 levels at FAN_IN=4.

Both strategies emit byte-identical streams in identical order,
verified by an FNV-1a hash over every dispatched event when run with
`mode == both`.

## Files

```
hard/
├── README.md
├── Makefile
├── bench.sh             ← timing harness (warmup + N runs, prints percentiles)
└── src/
    ├── main_hard.cpp    ← producers + flat / hierarchy mergers + dispatcher
    ├── spsc_ring.h      ← batched single-producer / single-consumer ring
    └── tournament.h     ← templated winner-tree k-way merge
```

The shared parser primitives live in `../common/`.

## Build

Dependencies:
- C++20 compiler with `<format>`
- POSIX `mmap` / `fcntl`
- pthreads

simdjson is not required. The Hard binary parses through
`fastparse::parse_line` only.

```bash
make            # builds ingest_hard
make rebuild
```

Compile flags: `-std=c++20 -O3 -march=native -flto -DNDEBUG -pthread`.

## Run

```bash
./ingest_hard <folder> [flat|hierarchy|both]
# or
make run FOLDER=/path/to/dir [MODE=flat|hierarchy|both]
```

The folder should contain ~20 daily Databento NDJSON files. Only files
matching `*.mbo.json` are picked up; sibling metadata files
(`manifest.json`, `symbology.json`, etc.) are skipped automatically.

Per strategy the binary prints:
- `Total messages processed`, `Wall time`, `Throughput`
- Data-quality counters: parse errors, dropped, fallback timestamps,
  `F_BAD_TS_RECV`, `F_MAYBE_BAD_BOOK`
- Cross-check fields: `Distinct instruments`, `LOB best-bid/ask updates`,
  `Event hash (FNV-1a)`

When `mode == both`, the two hashes are compared at the end. Mismatch
exits with code 2.

`PRINT_EVERY=N` controls how often `processMarketDataEvent` prints a
sample (default `1000000`; `PRINT_EVERY=1` prints every event, useful
for tiny inputs).
`CHUNK_MB=N` overrides the chunk-parallel parsing target (default 192;
set very high (e.g. 4096) to disable chunking).

## Architecture

```
Strategy 1: Flat
─────────────────
producer thread per chunk ┐     (mmap once per file →
  (line-aligned byte slice │      parse a line-aligned
   → ring)                 │      byte slice → ring)
            ┌──────────────┴──────────────┐
            │ N batched SPSC rings        │
            └──────────────┬──────────────┘
                           │
                  k-way tournament-tree
                  merge (single thread)
                           │
                  processMarketDataEvent
```

```
Strategy 2: Hierarchy (FAN_IN=4)
─────────────────────────────────
producer thread per chunk ┐
            ┌──────────────┴──────────────┐
            │ N batched SPSC rings        │
            └──┬────┬────┬────┬───────────┘
               │    │    │    │
            ┌──┴────┴──┐ ┌──┴────┴──┐    level 1: 4-way mergers
            │ 4-way    │ │ 4-way    │    (TournamentTree<Source,4>,
            │ merger   │ │ merger   │     2 best() calls per pop)
            └────┬─────┘ └────┬─────┘
                 │            │
              ┌──┴───┐ ... ──┴──┐
              │ 4-way merger     │      level 2: collapses level-1 outputs
              └──────────┬───────┘
                         │
                processMarketDataEvent
```

Files are split into ~`CHUNK_MB`-sized line-aligned byte ranges, so a
typical 22-file drop fans out into ~30 leaf chunks (largest files get
3–4 chunks, files ≤ `CHUNK_MB` stay as one). The hierarchy collapses
those ~30 leaves into ~3 levels at FAN_IN=4, running ~9 merger threads
total. `FAN_IN=N` overrides the fan-in.

## Benchmark — Apple M3 Pro (12 cores, 18 GB)

22-file folder `XEUR-20260409-HJTR7RCAKT` (4.9 GB on disk), 15.18 M
kept events, `PRINT_EVERY=100000000`. Numbers are best/median of 20
runs after a 6-run warmup, observed across two separate bench rounds.
The range is host load: lower bound at load avg 3–6, upper bound at
6–10.

| Strategy | Wall (best) | Wall (median) | Throughput (best) |
|---|---|---|---|
| Flat (k-way merge) | 1.80 – 2.32 s | 2.27 – 2.69 s | 6.5 – 8.4 M evt/s |
| Hierarchy (4-way fan-in) | 1.30 – 1.66 s | 1.72 – 2.34 s | 9.1 – 11.7 M evt/s |

Hierarchy is 25–30 % ahead of flat on best and ~24 % on median. The
4-way merger tree feeds events to the consumer with shorter
critical-path latency than a single dispatcher draining ~30 rings,
and with QoS hints on the merger threads the kernel keeps both the
merger and consumer pinned to the P-cluster so cache lines never
migrate.

End-to-end on hier-best (1.30 s): 11.7 M evt/s parsed + merged +
cross-check hashed, throughput ~3.8 GB/s through the full pipeline.

For the full optimization story see
[`../docs/perf_journey.md`](../docs/perf_journey.md).

## Cross-check

When `mode == both`, the same events flow through two different
pipelines. Each run independently FNV-1a-hashes every dispatched event
(key_ts_ns, ts_recv_ns, price, size, sequence, order_id,
instrument_id, flags, action, side, has_price). After both runs we
compare the two hashes plus the toy LOB's distinct-instrument count
and update count. Matching values prove the two strategies emit
byte-identical streams in identical order.

```
=== Cross-check (flat vs hierarchy) ===
OK — flat and hierarchy emit byte-identical streams.
```

## Bench harness

```bash
./bench.sh /path/to/folder both 20 6
```

Args: `folder`, `mode` (`flat` / `hierarchy` / `both`), `N` runs,
`WARMUP` runs (discarded). Prints
`min / p10 / p25 / med / avg / p75 / p90 / max` for each mode.
