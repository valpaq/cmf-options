// Schema-aware NDJSON parser for Databento Eurex EOBI MBO. Hot path
// memcmps fixed ,"key": prefixes; mismatches fall through to
// parse_line_generic. ParseContext caches days_from_civil per (y,m,d).
#pragma once

#include "parse.h"

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace fastparse {

struct ParseContext {
    int     cached_y = 0, cached_m = 0, cached_d = 0;
    int64_t cached_days_epoch = 0;
};

// Stops at the next ',' or '}' at the current nesting depth.
inline const char* skip_value(const char* p, const char* end) {
    int depth = 0;
    while (p < end) {
        switch (*p) {
            case '{': case '[': ++depth; ++p; continue;
            case '}': case ']':
                if (depth == 0) return p;
                --depth; ++p; continue;
            case ',':
                if (depth == 0) return p;
                ++p; continue;
            case '"':
                ++p;
                while (p < end && *p != '"') {
                    if (*p == '\\') ++p;
                    ++p;
                }
                if (p < end) ++p;
                continue;
            default:
                ++p;
        }
    }
    return p;
}

// MBO ints fit uint64; skip from_chars's locale/overflow plumbing.
inline uint64_t parse_uint_inline(const char*& p, const char* end) noexcept {
    uint64_t v = 0;
    while (p < end) {
        unsigned d = static_cast<unsigned>(*p) - '0';
        if (d > 9) break;
        v = v * 10 + d;
        ++p;
    }
    return v;
}

// p at opening '"'; returns past closing '"'. "YYYY-MM-DDTHH:MM:SS[.fffffffff]Z".
inline uint64_t parse_ts_inline(const char*& p, const char* end,
                                ParseContext* ctx = nullptr) noexcept {
    using ingest::days_from_civil;
    using ingest::swar_2digit;
    using ingest::swar_4digit;

    if (p >= end || *p != '"') return 0;
    ++p;

    if (end - p < 19) {
        while (p < end && *p != '"') ++p;
        if (p < end) ++p;
        return 0;
    }

    int y  = static_cast<int>(swar_4digit(p));
    int mo = static_cast<int>(swar_2digit(p +  5));
    int dy = static_cast<int>(swar_2digit(p +  8));
    int h  = static_cast<int>(swar_2digit(p + 11));
    int mi = static_cast<int>(swar_2digit(p + 14));
    int s  = static_cast<int>(swar_2digit(p + 17));
    p += 19;

    uint32_t ns = 0;
    if (p < end && *p == '.') {
        ++p;
        int digs = 0;
        while (p < end && digs < 9 && static_cast<unsigned>(*p - '0') <= 9) {
            ns = ns * 10 + static_cast<uint32_t>(*p - '0');
            ++p; ++digs;
        }
        while (digs < 9) { ns *= 10; ++digs; }
    }
    if (p < end && *p == 'Z') ++p;
    if (p < end && *p == '"') ++p;

    int64_t days;
    if (ctx && ctx->cached_y == y && ctx->cached_m == mo && ctx->cached_d == dy) {
        days = ctx->cached_days_epoch;
    } else {
        days = days_from_civil(y, mo, dy);
        if (ctx) {
            ctx->cached_y = y;
            ctx->cached_m = mo;
            ctx->cached_d = dy;
            ctx->cached_days_epoch = days;
        }
    }
    int64_t secs = days * 86400 + h * 3600 + mi * 60 + s;
    return static_cast<uint64_t>(secs) * 1'000'000'000ULL + ns;
}

inline void advance_to_next_line(const char*& p, const char* end) noexcept {
    const void* nl = std::memchr(p, '\n', static_cast<size_t>(end - p));
    p = nl ? static_cast<const char*>(nl) + 1 : end;
}

// Positional fast path. Field order:
//   {"ts_recv":"<TS>","hd":{"ts_event":"<TS>","rtype":N,"publisher_id":N,
//    "instrument_id":N},"action":"X","side":"X","price":<V>,"size":N,
//    "channel_id":N,"order_id":"<DIGITS>","flags":N,"ts_in_delta":N,
//    "sequence":N,"symbol":"..."}
// Failure may leave p partially advanced; parse_line() resets it.
inline bool parse_line_positional(const char*& p, const char* end,
                                  ingest::MarketDataEvent& out,
                                  ParseContext* ctx = nullptr) {
    using namespace ingest;

    if (end - p < 64) return false;

    if (memcmp(p, "{\"ts_recv\":", 11) != 0) return false;
    p += 11;
    const uint64_t ts_recv = parse_ts_inline(p, end, ctx);

    if (end - p < 18 || memcmp(p, ",\"hd\":{\"ts_event\":", 18) != 0) return false;
    p += 18;

    // ts_recv is in 100% of events on every measured drop. When non-zero
    // we skip ts_event with a fixed 32-byte offset + quote check; the
    // per-line memchr it replaces was ~10% of producer samples.
    uint64_t ts_event = 0;
    if (ts_recv != 0) {
        if (end - p < 32 || p[0] != '"' || p[31] != '"') return false;
        p += 32;
    } else {
        ts_event = parse_ts_inline(p, end, ctx);
    }

    out.ts_recv_ns = ts_recv;
    out.key_ts_ns  = ts_recv ? ts_recv : ts_event;

    if (end - p < 9 || memcmp(p, ",\"rtype\":", 9) != 0) return false;
    p += 9;
    while (p < end && static_cast<unsigned>(*p - '0') <= 9) ++p;

    if (end - p < 16 || memcmp(p, ",\"publisher_id\":", 16) != 0) return false;
    p += 16;
    while (p < end && static_cast<unsigned>(*p - '0') <= 9) ++p;

    if (end - p < 17 || memcmp(p, ",\"instrument_id\":", 17) != 0) return false;
    p += 17;
    out.instrument_id = static_cast<uint32_t>(parse_uint_inline(p, end));
    if (p < end && *p == '}') ++p;

    if (end - p < 13 || memcmp(p, ",\"action\":\"", 11) != 0) return false;
    out.action = static_cast<Action>(p[11]);
    p += 13;

    if (end - p < 11 || memcmp(p, ",\"side\":\"", 9) != 0) return false;
    out.side = static_cast<Side>(p[9]);
    p += 11;

    if (end - p < 9 || memcmp(p, ",\"price\":", 9) != 0) return false;
    p += 9;
    if (*p == 'n') {
        p += 4;
    } else if (*p == '"') {
        // Three formats: UNDEF sentinel (19 digits), 12-byte "0.dddddddddd"
        // (every non-null price in the drop), fallback "<digits>.<digits>".
        // 12-byte path is the hot case — replaces from_chars<double> at
        // ~10% of producer samples.
        ++p;
        bool consumed = false;
        if (end - p >= 20 && p[19] == '"' &&
            memcmp(p, UNDEF_PRICE_RAW.data(), 19) == 0) {
            p += 20;
            consumed = true;
        } else if (end - p >= 12 && p[0] == '0' && p[1] == '.' && p[11] == '"') {
            uint64_t frac = 0;
            bool valid = true;
            for (int i = 2; i < 11; ++i) {
                const unsigned d = static_cast<unsigned>(p[i]) - '0';
                if (d > 9) { valid = false; break; }
                frac = frac * 10 + d;
            }
            if (valid) {
                out.price     = static_cast<double>(frac) * 1e-9;
                out.has_price = true;
                p += 12;
                consumed = true;
            }
        }
        if (!consumed) {
            const char* q = p;
            while (q < end && *q != '"') ++q;
            if (std::memchr(p, '.', static_cast<size_t>(q - p))) {
                double d;
                if (std::from_chars(p, q, d).ec == std::errc{}) {
                    out.price     = d;
                    out.has_price = true;
                }
            } else {
                const char* save = p;
                double d = parse_price_digits(p, q);
                if (p > save) {
                    out.price     = d;
                    out.has_price = true;
                }
            }
            p = q;
            if (p < end && *p == '"') ++p;
        }
    } else {
        // bare numeric (pretty_px without quotes)
        auto [ptr, ec] = std::from_chars(p, end, out.price);
        if (ec == std::errc{}) out.has_price = true;
        p = ptr;
    }

    if (end - p < 8 || memcmp(p, ",\"size\":", 8) != 0) return false;
    p += 8;
    out.size = parse_uint_inline(p, end);

    // channel_id is constant 79 in every measured drop. Bulk-compare both
    // keys + the value + order_id opening quote in one 30-byte memcmp.
    static constexpr char kChanOrder[] = ",\"channel_id\":79,\"order_id\":\"";
    constexpr size_t kChanOrderLen = sizeof(kChanOrder) - 1;
    if (end - p < static_cast<ptrdiff_t>(kChanOrderLen) ||
        memcmp(p, kChanOrder, kChanOrderLen) != 0) return false;
    p += kChanOrderLen;
    uint64_t oid = 0;
    while (p < end && static_cast<unsigned>(*p - '0') <= 9) {
        oid = oid * 10 + static_cast<uint64_t>(*p - '0');
        ++p;
    }
    if (p < end && *p == '"') ++p;
    out.order_id = oid;

    if (end - p < 9 || memcmp(p, ",\"flags\":", 9) != 0) return false;
    p += 9;
    out.flags = static_cast<uint32_t>(parse_uint_inline(p, end));

    // ts_in_delta: skipped
    if (end - p < 15 || memcmp(p, ",\"ts_in_delta\":", 15) != 0) return false;
    p += 15;
    if (p < end && *p == '-') ++p;
    while (p < end && static_cast<unsigned>(*p - '0') <= 9) ++p;

    if (end - p < 12 || memcmp(p, ",\"sequence\":", 12) != 0) return false;
    p += 12;
    out.sequence = parse_uint_inline(p, end);

    // symbol: skipped
    advance_to_next_line(p, end);
    return true;
}

// Fallback for arbitrary field order or missing fields.
inline bool parse_line_generic(const char*& p, const char* end,
                               ingest::MarketDataEvent& out,
                               ParseContext* ctx = nullptr) {
    using namespace ingest;

    if (p >= end || *p != '{') return false;
    ++p;

    uint64_t ts_recv = 0, ts_event = 0;

    while (p < end) {
        while (p < end && (*p == ' ' || *p == '\t' || *p == ',')) ++p;
        if (p >= end || *p == '}') break;

        if (*p != '"') return false;
        const char* key_start = p + 1;
        const char* key_end   = key_start;
        while (key_end < end && *key_end != '"') ++key_end;
        if (key_end >= end) return false;

        const size_t klen = static_cast<size_t>(key_end - key_start);
        p = key_end + 1;

        while (p < end && (*p == ' ' || *p == ':')) ++p;
        if (p >= end) return false;

        const char c0 = key_start[0];

        switch (klen) {
        case 2:
            if (c0 == 'h') {
                // hd sub-object: capture ts_event and instrument_id.
                if (p >= end || *p != '{') return false;
                ++p;
                while (p < end && *p != '}') {
                    while (p < end && (*p == ',' || *p == ' ' || *p == '\t')) ++p;
                    if (p >= end || *p == '}') break;

                    if (*p != '"') return false;
                    const char* sk_s = p + 1;
                    const char* sk_e = sk_s;
                    while (sk_e < end && *sk_e != '"') ++sk_e;
                    if (sk_e >= end) return false;
                    const size_t sk_len = static_cast<size_t>(sk_e - sk_s);
                    p = sk_e + 1;

                    while (p < end && (*p == ' ' || *p == ':')) ++p;
                    if (p >= end) return false;

                    if (sk_len == 8 && memcmp(sk_s, "ts_event", 8) == 0) {
                        ts_event = parse_ts_inline(p, end, ctx);
                    } else if (sk_len == 13 && memcmp(sk_s, "instrument_id", 13) == 0) {
                        out.instrument_id = static_cast<uint32_t>(parse_uint_inline(p, end));
                    } else {
                        p = skip_value(p, end);
                    }
                }
                if (p < end && *p == '}') ++p;
                continue;
            }
            break;

        case 4:
            if (c0 == 's') {
                if (key_start[1] == 'i' && key_start[2] == 'z') {
                    out.size = parse_uint_inline(p, end);
                } else {
                    if (p < end && *p == '"') {
                        out.side = static_cast<Side>(p[1]);
                        p += 3;
                    } else {
                        p = skip_value(p, end);
                    }
                }
            } else {
                p = skip_value(p, end);
            }
            break;

        case 5:
            if (c0 == 'p') {
                if (*p == 'n') {
                    p += 4;
                    out.has_price = false;
                } else if (*p == '"') {
                    ++p;
                    if (end - p >= 20 && p[19] == '"' &&
                        memcmp(p, UNDEF_PRICE_RAW.data(), 19) == 0) {
                        p += 20;
                    } else {
                        const char* q = p;
                        while (q < end && *q != '"') ++q;
                        if (std::memchr(p, '.', static_cast<size_t>(q - p))) {
                            double d;
                            if (std::from_chars(p, q, d).ec == std::errc{}) {
                                out.price     = d;
                                out.has_price = true;
                            }
                        } else {
                            const char* save = p;
                            double d = parse_price_digits(p, q);
                            if (p > save) {
                                out.price     = d;
                                out.has_price = true;
                            }
                        }
                        p = q;
                        if (p < end && *p == '"') ++p;
                    }
                } else {
                    auto [ptr, ec] = std::from_chars(p, end, out.price);
                    if (ec == std::errc{}) out.has_price = true;
                    p = ptr;
                }
            } else if (c0 == 'f') {
                out.flags = static_cast<uint32_t>(parse_uint_inline(p, end));
            } else {
                p = skip_value(p, end);
            }
            break;

        case 6:
            if (c0 == 'a' && memcmp(key_start, "action", 6) == 0) {
                if (p < end && *p == '"') {
                    out.action = static_cast<Action>(p[1]);
                    p += 3;
                } else {
                    p = skip_value(p, end);
                }
            } else {
                p = skip_value(p, end);
            }
            break;

        case 7:
            if (c0 == 't' && memcmp(key_start, "ts_recv", 7) == 0) {
                ts_recv = parse_ts_inline(p, end, ctx);
            } else {
                p = skip_value(p, end);
            }
            break;

        case 8:
            if (c0 == 'o') {
                if (p < end && *p == '"') {
                    ++p;
                    uint64_t oid = 0;
                    while (p < end && static_cast<unsigned>(*p - '0') <= 9) {
                        oid = oid * 10 + static_cast<uint64_t>(*p - '0');
                        ++p;
                    }
                    if (p < end && *p == '"') ++p;
                    out.order_id = oid;
                } else {
                    p = skip_value(p, end);
                }
            } else if (c0 == 's') {
                out.sequence = parse_uint_inline(p, end);
            } else {
                p = skip_value(p, end);
            }
            break;

        default:
            p = skip_value(p, end);
            break;
        }
    }

    if (p < end && *p == '}') ++p;
    advance_to_next_line(p, end);

    out.ts_recv_ns = ts_recv;
    out.key_ts_ns  = ts_recv ? ts_recv : ts_event;
    return true;
}

// On success, p is past the trailing '\n'. On failure, nullptr; the
// caller should rewind to resync.
inline const char* parse_line(const char*& p, const char* end,
                              ingest::MarketDataEvent& out,
                              ParseContext* ctx = nullptr) {
    out = ingest::MarketDataEvent{};
    const char* start = p;
    if (parse_line_positional(p, end, out, ctx)) return p;

    out = ingest::MarketDataEvent{};
    p = start;
    if (parse_line_generic(p, end, out, ctx)) return p;

    return nullptr;
}

}
