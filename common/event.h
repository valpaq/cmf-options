// 64-byte MarketDataEvent. Shared by Standard and Hard.
#pragma once

#include <cstdint>
#include <format>
#include <iostream>
#include <string>

namespace ingest {

enum class Action : uint8_t {
    Add = 'A', Cancel = 'C', Clear = 'R', Trade = 'T',
    Modify = 'M', Fill = 'F', Unknown = '?'
};
enum class Side : uint8_t {
    Bid = 'B', Ask = 'A', None = 'N', Unknown = '?'
};

inline constexpr uint64_t UNDEF_TIMESTAMP = UINT64_MAX;
inline constexpr std::string_view UNDEF_PRICE_RAW = "9223372036854775807";

inline constexpr uint32_t F_BAD_TS_RECV    = 1u << 3;
inline constexpr uint32_t F_MAYBE_BAD_BOOK = 1u << 2;

inline uint64_t index_ts(uint64_t ts_recv_ns, uint64_t ts_event_ns) noexcept {
    return ts_recv_ns ? ts_recv_ns : ts_event_ns;
}

std::string format_timestamp(uint64_t ns);

struct MarketDataEvent {
    uint64_t key_ts_ns     = 0;
    uint64_t ts_recv_ns    = 0;
    double   price         = 0.0;
    uint64_t size          = 0;
    uint64_t sequence      = 0;
    uint64_t order_id      = 0;
    uint32_t instrument_id = 0;
    uint32_t flags         = 0;
    Action   action        = Action::Unknown;
    Side     side          = Side::Unknown;
    bool     has_price     = false;
    // 5 bytes tail padding

    void print(std::ostream& os) const {
        os << "timestamp=" << format_timestamp(key_ts_ns)
           << " | instrument=" << instrument_id
           << " | order_id=" << order_id
           << " | side=" << static_cast<char>(side)
           << " | price=";
        if (has_price)
            os << std::format("{:.9f}", price);
        else
            os << "null";
        os << " | size=" << size
           << " | action=" << static_cast<char>(action)
           << "\n";
    }
};

static_assert(sizeof(MarketDataEvent) == 64);

} 
