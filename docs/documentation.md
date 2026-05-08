# Databento Standards & Conventions

Common fields, enums, and types used across Databento market-data
schemas. Cleaned-up reference for the HW1 ingestion layer — only the
parts relevant to MBO L3 parsing are highlighted.

---

## Publishers, datasets, and venues

- **Dataset** — a source of data.
- **Venue** — an exchange, OTC market (ATS, ECN), or reporting entity.
- **Publisher** — a specific venue from a specific dataset.

### Publisher identifiers

Every schema includes `publisher_id`, a unique numeric ID Databento
assigns to each publisher. Full list via `metadata.list_publishers`.

### Venue and dataset identifiers

Each publisher also has a string ID (e.g. `OPRA.PILLAR.XCBO`):

- **Dataset ID** (e.g. `OPRA.PILLAR`) — used as the `dataset` argument
  in any API or client method. IDs are listed on each dataset's
  details page or via `metadata.list_datasets`.
- **Venue** (e.g. `XCBO`) — typically the ISO 10383 MIC code (always
  4 chars). For entities without a MIC, Databento assigns a synthetic
  4-char string.

### Instrument identifiers

`instrument_id` is a numeric ID mapping to a given instrument. Most of
the time it is publisher-assigned; otherwise Databento generates a
synthetic mapping.

> ⚠️ `instrument_id` is **only guaranteed to be unique within a single
> day**. Some publishers re-issue the same ID for different instruments
> on different days. For stable identity across days, use
> `raw_symbol` and the symbology API.

---

## Timestamps

All timestamps are nanoseconds since the UNIX epoch. Field names are
prefixed `ts_`; deltas are suffixed `_delta`.

| Field | Meaning |
|---|---|
| `ts_event` | Event timestamp — when the matching engine received the event (FIX tag 60). Provided by the publisher; non-monotonicity in the source is preserved. |
| `ts_in_delta` | Publisher-sending timestamp encoded as a delta from `ts_recv`. Sending timestamp = `ts_recv − ts_in_delta`. May be negative; `int32_t`, clamped to `INT32_MIN/MAX`. |
| `ts_recv` | Databento receive timestamp. UTC, sub-microsecond accuracy, hardware-timestamped on the NIC, GPS-PTP-synced, **monotonic per symbol**, leap-second-adjusted at session end. |
| `ts_out` | (Live only) Time before data leaves Databento gateways. Same GPS source as `ts_recv`; `ts_out − ts_recv` measures internal latency. |

**`UNDEF_TIMESTAMP`** = `UINT64_MAX` (`18446744073709551615`) denotes a
null/undefined timestamp.

If a market provides only one of `ts_event` / sending-timestamp,
Databento sets both to that value (so `ts_in_delta` becomes the
difference between `ts_recv` and `ts_event`).

### Index timestamp

Every schema has a primary timestamp used for sorting and symbology
lookup: `ts_recv` if present, otherwise `ts_event`. Historical data
is filtered on this index timestamp; in CSV/JSON it is the first
field, with `ts_event` second when both exist.

---

## Encodings

Databento supports **DBN**, **CSV**, and **JSON**.

- **DBN** — fast binary normalized format; default for live, historical
  streaming, and batch flat files. All official client libraries use
  DBN under the hood.
- **CSV / JSON** — supported by the batch download system (used here:
  HW1 input is NDJSON).

---

## Time zone, dates, and times

- All Databento data and site displays default to **UTC**.
- API parameters use **ISO 8601** (UTC unless specified).
- ISO 8601 reduced-precision is allowed; less-significant components
  may be omitted (e.g. `"2024-05"` ≡ `"2024-05-01T00:00:00"`).
- Any ISO 8601 parameter accepts an alternative: nanoseconds since the
  UNIX epoch.
- All timestamp parameters are **start-inclusive, end-exclusive**.

### Forward-filling end parameters

If `end` is omitted, Databento "rounds up" the start (only for
sub-second-resolution unspecified components):

| Start | Effective start | Effective end |
|---|---|---|
| `2024` | `2024-01-01T00:00:00` | `2025-01-01T00:00:00` |
| `2024-03` | `2024-03-01T00:00:00` | `2024-04-01T00:00:00` |
| `2024-03-10` | `2024-03-10T00:00:00` | `2024-03-11T00:00:00` |
| `2024-03-10T01` | `2024-03-10T01:00:00` | `2024-03-10T02:00:00` |
| `2024-03-10T00:01` | `2024-03-10T00:01:00` | `2024-03-10T00:02:00` |

So `start="2024-03"` (no `end`) covers all of March 2024.

---

## `rtype` — Record type discriminant

Every DBN record header has a `uint8_t rtype` indicating the record
structure. Each schema has exactly one `rtype`.

| Name | Hex | Decimal | Description |
|---|---|---|---|
| MBP-0 | `0x00` | 0 | Market-by-price, depth 0. Used for the **trades** schema. |
| MBP-1 | `0x01` | 1 | Market-by-price, depth 1. TBBO and MBP-1 schemas. |
| MBP-10 | `0x0A` | 10 | Market-by-price, depth 10. |
| Status | `0x12` | 18 | Exchange status record. |
| Definition | `0x13` | 19 | Instrument definition. |
| Imbalance | `0x14` | 20 | Order imbalance. |
| Error | `0x15` | 21 | Error from the live gateway. |
| Symbol mapping | `0x16` | 22 | Symbol mapping (live). |
| System | `0x17` | 23 | Non-error system message (live). |
| Statistics | `0x18` | 24 | Publisher statistics. |
| OHLCV-1s | `0x20` | 32 | OHLCV at 1s cadence. |
| OHLCV-1m | `0x21` | 33 | OHLCV at 1m cadence. |
| OHLCV-1h | `0x22` | 34 | OHLCV at 1h cadence. |
| OHLCV-1d | `0x23` | 35 | OHLCV at 1d cadence. |
| **MBO** | `0xA0` | **160** | **Market-by-order** ← HW1 input. |
| CMBP-1 | `0xB1` | 177 | Consolidated MBP, depth 1. |
| CBBO-1s | `0xC0` | 192 | Consolidated MBP, depth 1, 1s cadence. |
| CBBO-1m | `0xC1` | 193 | Consolidated MBP, depth 1, 1m cadence. |
| TCBBO | `0xC2` | 194 | Consolidated MBP, depth 1, trades only. |
| BBO-1s | `0xC3` | 195 | MBP depth 1 at 1s cadence. |
| BBO-1m | `0xC4` | 196 | MBP depth 1 at 1m cadence. |

> HW1 records all carry `rtype = 160` (MBO).

---

## Prices

Prices are signed integers in **fixed-precision** format: 1 unit =
`1e-9` (one nanounit). Example: `5411750000000` ≡ `5411.75`.

When requesting batch data in CSV/JSON you may opt into decimal format
(via `pretty_px` in `batch.submit_job`, or via the portal's
"Decimal prices" advanced option). Some prices can be **negative**
(e.g. calendar spreads in futures).

**`UNDEF_PRICE`** = `INT64_MAX` = `9223372036854775807` denotes a
null/undefined price. In decimal format it appears as JSON `null` /
empty string in CSV.

---

## Side

The `side` field meaning depends on `action`:

| `action` | `side='A'` | `side='B'` | `side='N'` |
|---|---|---|---|
| Trade | Trade aggressor was a **seller** | Trade aggressor was a **buyer** | No side |
| Fill | Resting **sell** order filled | Resting **buy** order filled | No side |
| Add / Modify / Cancel | Resting **sell** order updates the book | Resting **buy** order updates the book | No side |
| Clear | — | — | Always `N` |

### When `side` may be `N`

- The source does not disseminate a side for trades.
- Trades during opening / closing auctions.
- Trades against non-displayed orders.
- Trades involving implied orders.
- Off-exchange trades.

Per-dataset specifics are documented in *Venues and datasets*.

---

## Action

| Name | Value | Effect |
|---|---|---|
| Add | `A` | Insert a new order into the book. |
| Modify | `M` | Change an order's price and/or size. |
| Cancel | `C` | Fully or partially cancel an order. |
| Clear | `R` | Remove all resting orders for the instrument. |
| Trade | `T` | Aggressing order traded. **Does not affect the book.** |
| Fill | `F` | Resting order was filled. **Does not affect the book.** |
| None | `N` | No action. May carry flags or other info. |

---

## Flags

`flags` is a bit field. Multiple flags may be set on one message.

| Flag | Bit | Decimal | Meaning |
|---|---|---|---|
| `F_LAST` | `1 << 7` | 128 | Last record in a single event for a given `instrument_id`. |
| `F_TOB` | `1 << 6` | 64 | Top-of-book message, not an individual order. |
| `F_SNAPSHOT` | `1 << 5` | 32 | Sourced from a replay (e.g. snapshot server). |
| `F_MBP` | `1 << 4` | 16 | Aggregated price-level message, not an individual order. |
| `F_BAD_TS_RECV` | `1 << 3` | 8 | `ts_recv` is inaccurate (clock issues / packet reordering). |
| `F_MAYBE_BAD_BOOK` | `1 << 2` | 4 | Unrecoverable gap detected in the channel. |
| `F_PUBLISHER_SPECIFIC` | `1 << 1` | 2 | Semantics depend on `publisher_id` (see dataset supplement). |
| (reserved) | `1 << 0` | 1 | Reserved for internal use; safely ignored. May be set or unset. |

---

## Top-of-book datasets

Datasets fed by top-of-book-only vendors are normalized into pairs of
**MBO records with `Add` action and `F_TOB` set (`0x40`, 64)**.
Typically there is no information about the passive side of trades, so
no `Fill` records, and `Trade` records always have `side = N`.

A price-level removal is encoded as **`Add` with `size = 0` and
`price = UNDEF_PRICE`** (`INT64_MAX`, or `NaN` in Python). This signals
"no quote on that side."

Other schemas (MBP-1, Trades, OHLCV) are unchanged for these datasets.

---

## Market-by-price datasets

Datasets fed by MBP-only vendors (limited depth) are normalized into
**MBO records with `Add` / `Modify` / `Cancel` actions, the `size`
field carrying the full quantity at that level, and `F_MBP` set
(`0x10`, 16)**. A price level is identified by `(side, price)`; the
`order_id` field should be ignored.

If the upstream feed has a maximum depth, an extra `Cancel` is sent
when a level falls outside that depth — even if orders still rest at
that price.

There is typically no information about the passive side of trades, so
no `Fill` records.

`MBP-10` only includes depth up to what the publisher provides; deeper
levels are always empty.

Other schemas (Trades, OHLCV) are unaffected.
