# HW1 — Data Ingestion Layer (C++)

Two graded tasks share the same parsing layer. Standard is the
single-file warm-up; Hard is the multi-file producer + merger pipeline.

## Standard Task

### Objective

Build the basic data-ingestion layer of our event-driven backtester.
You will read one daily JSON L3 file, parse each message in
chronological order, and create `MarketDataEvent` objects that will
later feed the Limit Order Book (LOB) engine.

### Task

1. Implement the `MarketDataEvent` class.
2. Write a program that:
   - Takes one command-line argument: path to a single daily JSON file
     (e.g. `2025-04-01_EURUSD_options_12345.json`).
   - Reads the file line-by-line (NDJSON).
   - For every valid message, creates a `MarketDataEvent` instance.
   - Passes the `MarketDataEvent` to a consumer function
     `processMarketDataEvent(const MarketDataEvent& order)` that simply
     prints some of the events (timestamp, order_id, side, price, size,
     action) for verification.
3. Process the entire file and print a short summary at the end (total
   messages processed, first and last timestamp). Output the first 10
   and last 10 `MarketDataEvent` objects created.

## Hard Task

### Objective

Build a scalable data-ingestion layer for our C++ event-driven
backtester using real L3 JSON files from Eurex EUR/USD options and
futures. You will merge multiple daily files into one chronological
stream using two different merging strategies, measure their
performance, and feed events to a single dispatcher thread.

### Task

1. Implement the `MarketDataEvent` class (see skeleton).
2. Write a program that:
   - Takes a folder path containing ~20 daily Databento NDJSON L3 files
     (options or futures).
   - Uses multiple producer threads (one per file) to read and parse
     the JSON messages. Each file is already chronologically sorted
     internally.
   - Merges all messages into one single chronological queue using two
     different strategies (see below).
   - Launches one dispatcher thread that reads sequentially from the
     merged queue, creates `MarketDataEvent` objects, and calls
     `processMarketDataEvent(const MarketDataEvent& event)`.
3. The `processMarketDataEvent` function should print the event details
   (for now) and will later be responsible for updating the
   corresponding LOB.

### Merging Strategies — Both Required

1. **Flat Merger (single-level k-way merge)** — one queue that holds
   the next pending event from each of the ~20 files. Classic efficient
   approach for a moderate number of input streams.
2. **Hierarchy Merger (multi-level tree-based merge)** — a binary (or
   4-way) tree of mergers. First merge files in pairs/groups, then
   merge the resulting intermediate streams, until you have one final
   chronological stream. May use multiple smaller priority queues or a
   recursive approach.

### For Both Mergers

- Preserve perfect global chronological order across all files.
- Use the same per-file producer threads for reading/parsing.
- After implementing both, run a benchmark on the full set of ~20
  files and measure:
  - Total messages processed
  - Wall-clock time (seconds)
  - Throughput (messages per second)

Databento format reference: [`documentation.md`](documentation.md).
