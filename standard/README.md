# Standard task — single-file NDJSON ingestion

Reads one daily Eurex EOBI MBO NDJSON file, builds `MarketDataEvent`
objects per line, feeds them to `processMarketDataEvent`, prints the
first 10 / last 10 events plus a summary.

Two binaries built from the same data path:

| Binary | Parser | Purpose |
|---|---|---|
| `ingest_opt` | simdjson `ondemand` | Reference / correctness oracle |
| `ingest_fast` | custom schema-aware (`../common/fast_parse.h`) | Recommended fast path |

Both produce byte-identical output (same first/last timestamps, same
flag counters, same total). Verified end-to-end.

## Files

```
standard/
├── README.md
├── Makefile
└── src/
    ├── main.cpp              ← ingest_opt  (simdjson variant)
    ├── main_fast.cpp         ← ingest_fast (custom-parser variant)
    └── parse_simdjson.h      ← simdjson `parse_event` (used only by ingest_opt)
```

The shared parser primitives live in `../common/`:

```
common/
├── event.h        ← canonical 64-byte MarketDataEvent + enums + flag bits
├── parse.h        ← SWAR helpers, civil-calendar math, mmap loader, summary
└── fast_parse.h   ← schema-aware NDJSON parser (no simdjson dependency)
```

## Build

Dependencies:
- C++20 compiler with `<format>`
- `simdjson` (auto-detected via `pkg-config`; defaults to `-lsimdjson`)

```bash
make            # builds both
make opt        # ingest_opt only
make fast       # ingest_fast only
make rebuild
```

## Run

```bash
./ingest_opt  /path/to/2026-04-01_xeur-eobi.mbo.json
./ingest_fast /path/to/2026-04-01_xeur-eobi.mbo.json
# or
make run      FILE=/path/to/file.ndjson
make run-fast FILE=/path/to/file.ndjson
```

`PRINT_EVERY=N` controls how often `processMarketDataEvent` prints a
sample (default `100000`; `PRINT_EVERY=1` prints every event). Same env
var as the Hard binary (its default is `1000000` because it processes
~20× more events).

## Benchmark — Apple M3 Pro

One day of Eurex EOBI MBO (`xeur-eobi-20260401.mbo.json`, 263 MB,
~796 K kept events; the bulk of the file is `R` clear records that
arrive without timestamps and would normally be filtered out at a
later stage):

| Variant | Wall (best) | Throughput |
|---|---|---|
| `ingest_opt` (simdjson `ondemand`) | 0.108 s | ~7.4 M evt/s |
| `ingest_fast` (custom parser) | 0.042 s | ~19 M evt/s |

`ingest_fast` wins ~2.5×. Per-thread parser ceiling on this hardware
is ~6 GB/s through the schema-aware path; the Hard task hits this on
its busiest producer thread before chunk-parallelism kicks in.

For the optimization story see [`../docs/perf_journey.md`](../docs/perf_journey.md).
