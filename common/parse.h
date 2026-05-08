// Shared NDJSON/MBO primitives. Hot path: fast_parse.h. simdjson path:
// standard/src/parse_simdjson.h.
#pragma once

#include "event.h"

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <format>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

namespace ingest {

inline int64_t days_from_civil(int y, int m, int d) noexcept {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399LL) / 400;
    const int yoe = y - static_cast<int>(era) * 400;
    const int doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// SWAR: load 2/4 ASCII digits, subtract '0', fold by powers of ten.
inline uint32_t swar_2digit(const char* p) noexcept {
    uint16_t w; std::memcpy(&w, p, 2); w -= 0x3030;
    return (w & 0xFF) * 10 + ((w >> 8) & 0xFF);
}
inline uint32_t swar_4digit(const char* p) noexcept {
    uint32_t w; std::memcpy(&w, p, 4); w -= 0x30303030u;
    return ( w        & 0xFF) * 1000
         + ((w >>  8) & 0xFF) * 100
         + ((w >> 16) & 0xFF) * 10
         +  (w >> 24);
}

// YYYY-MM-DDTHH:MM:SS[.fffffffff]Z. Hot-path variant in fast_parse.h.
inline uint64_t parse_timestamp_ns(std::string_view sv) {
    if (sv.size() < 19) return 0;
    const char* p = sv.data();

    int y  = static_cast<int>(swar_4digit(p));
    int mo = static_cast<int>(swar_2digit(p + 5));
    int dy = static_cast<int>(swar_2digit(p + 8));
    int h  = static_cast<int>(swar_2digit(p + 11));
    int mi = static_cast<int>(swar_2digit(p + 14));
    int s  = static_cast<int>(swar_2digit(p + 17));

    uint32_t ns = 0;
    if (sv.size() > 19 && sv[19] == '.') {
        size_t i = 20;
        int digs = 0;
        while (i < sv.size() && sv[i] >= '0' && sv[i] <= '9' && digs < 9) {
            ns = ns * 10 + (sv[i] - '0');
            ++i; ++digs;
        }
        while (digs < 9) { ns *= 10; ++digs; }
    }
    int64_t days = days_from_civil(y, mo, dy);
    int64_t secs = days * 86400 + h * 3600 + mi * 60 + s;
    return static_cast<uint64_t>(secs) * 1'000'000'000ULL + ns;
}

inline std::string format_timestamp(uint64_t ns) {
    int64_t total_sec = static_cast<int64_t>(ns / 1'000'000'000);
    uint32_t nanos = static_cast<uint32_t>(ns % 1'000'000'000);

    int64_t days = total_sec / 86400;
    int rem = static_cast<int>(total_sec % 86400);
    int h = rem / 3600; rem -= h * 3600;
    int m = rem / 60;   int s = rem - m * 60;

    days += 719468;
    const int64_t era = (days >= 0 ? days : days - 146096) / 146097;
    const int doe = static_cast<int>(days - era * 146097);
    const int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = yoe + static_cast<int>(era) * 400;
    const int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int mp = (5 * doy + 2) / 153;
    const int d = doy - (153 * mp + 2) / 5 + 1;
    const int mo = mp + (mp < 10 ? 3 : -9);
    y += (mo <= 2);

    return std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:09d}Z",
                       y, mo, d, h, m, s, nanos);
}

// Databento price: JSON string holds the integer count of 1e-9 units.
inline double parse_price_str(std::string_view sv) noexcept {
    if (sv.empty()) return std::numeric_limits<double>::quiet_NaN();
    double result = 0.0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    return (ec == std::errc{}) ? result
                               : std::numeric_limits<double>::quiet_NaN();
}

// ~2× faster than from_chars<double> on integer-valued input — skips
// the FP fast/slow path. Caller handles the trailing quote.
inline double parse_price_digits(const char*& p, const char* end) noexcept {
    bool neg = false;
    if (p < end && *p == '-') { neg = true; ++p; }
    uint64_t v = 0;
    while (p < end) {
        unsigned d = static_cast<unsigned>(*p) - '0';
        if (d > 9) break;
        v = v * 10 + d;
        ++p;
    }
    double dv = static_cast<double>(v);
    return neg ? -dv : dv;
}

inline Action parse_action(std::string_view sv) {
    if (sv.size() != 1) return Action::Unknown;
    switch (sv[0]) {
        case 'A': return Action::Add;
        case 'C': return Action::Cancel;
        case 'R': return Action::Clear;
        case 'T': return Action::Trade;
        case 'M': return Action::Modify;
        case 'F': return Action::Fill;
        default:  return Action::Unknown;
    }
}

inline Side parse_side(std::string_view sv) {
    if (sv.size() != 1) return Side::Unknown;
    switch (sv[0]) {
        case 'B': return Side::Bid;
        case 'A': return Side::Ask;
        case 'N': return Side::None;
        default:  return Side::Unknown;
    }
}

struct MappedFile {
    char*  base     = nullptr;
    size_t size     = 0;
    size_t map_size = 0;

    ~MappedFile() {
        if (base && base != MAP_FAILED) ::munmap(base, map_size);
    }
    MappedFile() = default;
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    MappedFile(MappedFile&& o) noexcept
        : base(o.base), size(o.size), map_size(o.map_size) {
        o.base = nullptr; o.size = 0; o.map_size = 0;
    }
};

// Maps file + a trailing zero page so parsers can read past EOF safely
// (≥ SIMDJSON_PADDING).
inline int load_mmap(const char* path, MappedFile& out) {
    int fd = ::open(path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat sb;
    if (::fstat(fd, &sb) < 0) { ::close(fd); return -1; }
    size_t file_size = static_cast<size_t>(sb.st_size);

    if (file_size == 0) {
        ::close(fd);
        out.base = nullptr;
        out.size = 0;
        out.map_size = 0;
        return 0;
    }

    long ps = ::sysconf(_SC_PAGESIZE);
    size_t page_size  = (ps > 0) ? static_cast<size_t>(ps) : 4096;
    size_t file_pages = (file_size + page_size - 1) / page_size;
    size_t map_size   = file_pages * page_size + page_size;

    char* base = static_cast<char*>(::mmap(nullptr, map_size, PROT_READ,
                                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (base == MAP_FAILED) { ::close(fd); return -1; }

    char* fmap = static_cast<char*>(::mmap(base, file_size, PROT_READ,
                                           MAP_PRIVATE | MAP_FIXED | MAP_POPULATE,
                                           fd, 0));
    ::close(fd);
    if (fmap == MAP_FAILED) { ::munmap(base, map_size); return -1; }

    ::madvise(base, file_size, MADV_SEQUENTIAL);
    ::madvise(base, file_size, MADV_WILLNEED);

    out.base     = base;
    out.size     = file_size;
    out.map_size = map_size;
    return 0;
}

struct ParseSummary {
    uint64_t total            = 0;
    uint64_t parse_errors     = 0;
    uint64_t dropped_no_ts    = 0;
    uint64_t fallback_ts_event = 0;
    uint64_t bad_ts_recv      = 0;
    uint64_t maybe_bad_book   = 0;
    uint64_t first_ts         = UINT64_MAX;
    uint64_t last_ts          = 0;
    double   elapsed          = 0.0;
};

inline void print_summary(std::ostream& os, const ParseSummary& s) {
    if (s.total == 0) {
        os << "\n=== Summary ===\n";
        os << "Total messages processed: 0\n";
        os << std::format("Parse errors:             {}\n", s.parse_errors);
        return;
    }

    os << "\n=== Summary ===\n";
    os << std::format("Total messages processed: {}\n", s.total);
    os << std::format("Parse errors:             {}\n", s.parse_errors);
    os << std::format("Dropped (no usable ts):   {}\n", s.dropped_no_ts);
    os << std::format("Used ts_event fallback:   {}\n", s.fallback_ts_event);
    os << std::format("F_BAD_TS_RECV (flag 8):   {}\n", s.bad_ts_recv);
    os << std::format("F_MAYBE_BAD_BOOK (flag 4):{}\n", s.maybe_bad_book);
    if (s.first_ts == UINT64_MAX)
        os << "First timestamp:          n/a\n";
    else
        os << std::format("First timestamp:          {}\n", format_timestamp(s.first_ts));
    if (s.last_ts == 0)
        os << "Last timestamp:           n/a\n";
    else
        os << std::format("Last timestamp:           {}\n", format_timestamp(s.last_ts));
    os << std::format("Wall time:                {:.3f}s\n", s.elapsed);
    os << std::format("Throughput:               {:.0f} msg/s\n", s.total / s.elapsed);
}

}
