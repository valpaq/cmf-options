# Performance journey

Apple M3 Pro (8P + 4E), 18 GB. Workload: 22 daily Eurex MBO files,
4.9 GB, 15.18 M kept events. Best-of-20 wall, warm page cache.

Cross-check FNV-1a: `0x5d877913a896e16d`. Identical across flat/hier
on every run.

## Numbers

| | Wall (best) | Throughput |
|---|---|---|
| Hard — Hierarchy | 1.30 – 1.66 s | 9.1 – 11.7 M evt/s |
| Hard — Flat | 1.80 – 2.32 s | 6.5 – 8.4 M evt/s |
| Standard — `ingest_fast` | 0.042 s | 19 M evt/s |
| Standard — `ingest_opt` (simdjson) | 0.108 s | 7.4 M evt/s |

Range is host load (load avg 3–6 lower bound, 6–10 upper).

## What worked

1. **SPSC ring 128 → 8192 events × 64 slots** — Hier 1.25 → 0.50 s.
   Producers are bursty, consumer is steady; deeper rings absorb the gap.
2. **Tournament tree replaces `std::priority_queue`** — Flat 5.15 → 2.0 s.
   Straight-line `log₂(MaxLeaves)` walk, no allocator hits.
3. **Schema-aware fastparse + price/ts_event/channel_id fast-paths** —
   Hier ~10–19 %. 12-byte price, 32-byte `ts_event`, 30-byte combined
   memcmp for `channel_id` + `order_id`. All guarded; falls through to
   generic on mismatch.
4. **Chunk-parallel parsing** — Hier −15 to −18 %. Largest file (487 MB)
   was wall-time floor; split into 192 MB line-aligned chunks across
   multiple producer threads. Shared mmap, no extra I/O.
5. **4-way hierarchy fan-in** — −20 to −40 ms. Was binary tree (5 levels,
   29 mergers); now 4-way (3 levels, 9 mergers). Less oversubscription on 12 cores.
6. **QoS pinning** — −50 ms. Consumer + mergers `USER_INTERACTIVE`
   (P-cluster); producers `USER_INITIATED` (P-preferred, can spill to E).
7. **64-byte event** — was 96. Dropped `rtype`, `publisher_id`,
   `channel_id`, `ts_in_delta` from struct (still parsed if needed).
8. **Zero-copy producer + pointer-peek consumer** —
   `BatchProducer::next()` returns ring slot ref; `BatchConsumer::peek()`
   returns pointer. Source caches comparator fields locally.
9. **Two-stage flat dispatcher (cross-check only)** — k-way merge on its
   own thread → final SPSC ring → main thread runs FNV/LOB. Single-mode
   keeps merge inline.
10. **Conditional FNV/LOB + slim FNV** — skip both when `mode != both`.
    One xor + one multiply per field; byte-unrolled was 8× the work for
    no collision benefit.
11. **Hybrid SPSC backoff** — 32× pause, 64× yield, then 50 µs sleep.
    Yield-only had 10× variance under 33-thread oversubscription.
12. **`static` instead of `thread_local` for cross-check state** — main
    thread is the only caller; removes `_tlv_get_addr` indirection.

## What didn't ship

- **`condition_variable` BoundedQueue** — capped flat at ~4 M evt/s.
- **Yield-only spin** — 10× run-to-run variance on 12 cores.
- **Smaller ring (`<64,16>`)** to fit L2 — flat 5.15 → 8.5 s. Deeper, not
  shallower.
- **Open-addressed `LobTable`** (4096 × 32 B) — Hier −10 %. With ~1500
  live entries, `unordered_map`'s heap pool stays in L1; static table
  spreads across L2.
- **Producer fusion (parse N files per thread)** — uniformly slower.
  Forces an extra copy and pushed per-event throughput below consumer
  drain rate. Chunk-parallel solved the same problem the right way.
- **`atomic<uint64_t>` count** — single-threaded dispatcher; pure waste.
- **31-byte `rtype` + `publisher_id` memcmp** — within noise.
- **NEON 9-digit price parse** — current serial loop is ~9 cycles; not
  worth the shuffle/multiply setup at current sample share.

## Still leaving cycles

- Process-stage as third thread — only worth it if cross-check ever does
  real work. Today's 10–15 ms cost is below cross-cluster transfer overhead.
- `io_uring` + `O_DIRECT` (Linux) — 20–30 % on cold cache; macOS has no
  equivalent.
- NEON integer parse for `sequence` / `size` — pending, depends on
  compiler auto-vec.
- Right-sizing per-instrument LOB — only matters once toy LOB grows.

## Method

Each change A/B'd: build `mod` and `head` binaries, alternating runs
× N after warmup. Reject anything that doesn't move best-of-N or median
past ±5 % run-to-run noise. Cross-check hash gates every change — perf
wins that drift the hash get reverted.
