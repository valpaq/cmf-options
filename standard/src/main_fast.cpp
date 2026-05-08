// Standard task fast variant. Same output as main.cpp; parses each
// NDJSON line via fastparse::parse_line (no simdjson dependency).

#include "../../common/parse.h"
#include "../../common/fast_parse.h"

#include <array>
#include <cstring>
#include <iostream>
#include <vector>

using namespace ingest;

void processMarketDataEvent(const MarketDataEvent& event) {
    static const uint64_t every = []{
        const char* s = std::getenv("PRINT_EVERY");
        const uint64_t v = (s && *s) ? std::strtoull(s, nullptr, 10) : 100'000ULL;
        return v ? v : 1;
    }();
    static uint64_t count = 0;
    if (++count % every == 0) {
        event.print(std::cout);
    }
}

int main(int argc, char* argv[]) {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(nullptr);

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <path_to_ndjson_file>\n";
        return 1;
    }
    const char* filename = argv[1];

    MappedFile mf;
    if (load_mmap(filename, mf) != 0) {
        std::cerr << "Error loading " << filename << "\n";
        return 1;
    }
    std::cerr << "Loaded " << mf.size << " bytes\n";

    auto t0 = std::chrono::high_resolution_clock::now();

    std::array<MarketDataEvent, 10> ring{};
    size_t ring_idx = 0;
    bool ring_filled = false;

    std::vector<MarketDataEvent> first_10;
    first_10.reserve(10);

    uint64_t total = 0, parse_errors = 0;
    uint64_t first_ts = UINT64_MAX, last_ts = 0;
    uint64_t bad_ts_recv = 0, maybe_bad_book = 0;
    uint64_t fallback_ts_event = 0, dropped_no_ts = 0;

    const char* p   = mf.base;
    const char* end = mf.base + mf.size;

    fastparse::ParseContext ctx;

    while (p < end) {
        const char* saved = p;
        MarketDataEvent evt{};
        const char* next = fastparse::parse_line(p, end, evt, &ctx);
        if (!next) {
            parse_errors++;
            p = saved + 1;
            continue;
        }

        const uint64_t key = evt.key_ts_ns;
        if (key == 0 || key == UNDEF_TIMESTAMP) {
            dropped_no_ts++;
        } else {
            if (evt.ts_recv_ns == 0) fallback_ts_event++;
            if (evt.flags & F_BAD_TS_RECV)    bad_ts_recv++;
            if (evt.flags & F_MAYBE_BAD_BOOK) maybe_bad_book++;

            total++;
            if (key < first_ts) first_ts = key;
            if (key > last_ts)  last_ts  = key;

            processMarketDataEvent(evt);

            if (total <= 10) first_10.push_back(evt);

            ring[ring_idx] = evt;
            ring_idx = (ring_idx + 1) % 10;
            if (total >= 10) ring_filled = true;
        }

    }

    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "\n=== First 10 events ===\n";
    for (const auto& e : first_10) e.print(std::cout);

    std::cout << "\n=== Last 10 events ===\n";
    size_t start = ring_filled ? ring_idx : 0;
    size_t count = (total < 10) ? total : 10;
    for (size_t i = 0; i < count; ++i) {
        ring[(start + i) % 10].print(std::cout);
    }

    ParseSummary summary{
        .total = total,
        .parse_errors = parse_errors,
        .dropped_no_ts = dropped_no_ts,
        .fallback_ts_event = fallback_ts_event,
        .bad_ts_recv = bad_ts_recv,
        .maybe_bad_book = maybe_bad_book,
        .first_ts = first_ts,
        .last_ts = last_ts,
        .elapsed = elapsed,
    };
    print_summary(std::cout, summary);

    return 0;
}
