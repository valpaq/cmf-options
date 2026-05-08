#include "../common/event.h"
#include "../common/fast_parse.h"
#include "../common/parse.h"
#include "../hard/src/chunk.h"
#include "../hard/src/spsc_ring.h"
#include "../hard/src/tournament.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace ingest;

static int g_failed = 0;
static int g_passed = 0;

#define EXPECT(cond) do {                                                    \
    if (cond) { ++g_passed; }                                                \
    else {                                                                   \
        ++g_failed;                                                          \
        std::cerr << "FAIL " << __FILE__ << ":" << __LINE__                  \
                  << " " #cond "\n";                                         \
    }                                                                        \
} while (0)

#define EXPECT_EQ(a, b) do {                                                 \
    const auto _av = (a); const auto _bv = (b);                              \
    if (_av == _bv) { ++g_passed; }                                          \
    else {                                                                   \
        ++g_failed;                                                          \
        std::cerr << "FAIL " << __FILE__ << ":" << __LINE__                  \
                  << " " #a " == " #b                                        \
                  << " (got " << +_av << " vs " << +_bv << ")\n";            \
    }                                                                        \
} while (0)

static std::string golden_line() {
    return std::string(
        "{\"ts_recv\":\"2026-04-01T07:00:00.123456789Z\","
        "\"hd\":{\"ts_event\":\"2026-04-01T07:00:00.000000000Z\","
        "\"rtype\":160,\"publisher_id\":42,\"instrument_id\":12345},"
        "\"action\":\"A\",\"side\":\"B\",\"price\":\"0.001234567\","
        "\"size\":100,\"channel_id\":79,\"order_id\":\"9876543210\","
        "\"flags\":0,\"ts_in_delta\":1234,\"sequence\":555,"
        "\"symbol\":\"FOO\"}\n");
}

static void test_parse_positional_golden() {
    const std::string line = golden_line();
    const char* p   = line.data();
    const char* end = line.data() + line.size();
    MarketDataEvent evt;
    fastparse::ParseContext ctx;
    EXPECT(fastparse::parse_line(p, end, evt, &ctx) != nullptr);

    const uint64_t expected_ts =
        parse_timestamp_ns("2026-04-01T07:00:00.123456789Z");
    EXPECT_EQ(evt.ts_recv_ns,    expected_ts);
    EXPECT_EQ(evt.key_ts_ns,     expected_ts);
    EXPECT_EQ(evt.instrument_id, 12345u);
    EXPECT_EQ(static_cast<char>(evt.action), 'A');
    EXPECT_EQ(static_cast<char>(evt.side),   'B');
    EXPECT(evt.has_price);
    EXPECT(std::abs(evt.price - 0.001234567) < 1e-12);
    EXPECT_EQ(evt.size,     100ULL);
    EXPECT_EQ(evt.order_id, 9876543210ULL);
    EXPECT_EQ(evt.flags,    0u);
    EXPECT_EQ(evt.sequence, 555ULL);
}

static void test_parse_undef_price() {
    std::string line(
        "{\"ts_recv\":\"2026-04-01T07:00:00.000000000Z\","
        "\"hd\":{\"ts_event\":\"2026-04-01T07:00:00.000000000Z\","
        "\"rtype\":160,\"publisher_id\":42,\"instrument_id\":1},"
        "\"action\":\"R\",\"side\":\"N\","
        "\"price\":\"9223372036854775807\","
        "\"size\":0,\"channel_id\":79,\"order_id\":\"0\","
        "\"flags\":0,\"ts_in_delta\":0,\"sequence\":1,"
        "\"symbol\":\"X\"}\n");
    const char* p = line.data();
    MarketDataEvent evt;
    fastparse::ParseContext ctx;
    EXPECT(fastparse::parse_line(p, line.data() + line.size(), evt, &ctx) != nullptr);
    EXPECT(!evt.has_price);
    EXPECT_EQ(static_cast<char>(evt.action), 'R');
    EXPECT_EQ(static_cast<char>(evt.side),   'N');
}

static void test_parse_null_price() {
    std::string line(
        "{\"ts_recv\":\"2026-04-01T07:00:00.000000000Z\","
        "\"hd\":{\"ts_event\":\"2026-04-01T07:00:00.000000000Z\","
        "\"rtype\":160,\"publisher_id\":42,\"instrument_id\":1},"
        "\"action\":\"R\",\"side\":\"N\",\"price\":null,"
        "\"size\":0,\"channel_id\":79,\"order_id\":\"0\","
        "\"flags\":0,\"ts_in_delta\":0,\"sequence\":1,"
        "\"symbol\":\"X\"}\n");
    const char* p = line.data();
    MarketDataEvent evt;
    fastparse::ParseContext ctx;
    EXPECT(fastparse::parse_line(p, line.data() + line.size(), evt, &ctx) != nullptr);
    EXPECT(!evt.has_price);
}

static void test_parse_generic_reordered() {
    // Field reorder forces the positional path to bail; the line must
    // still parse via parse_line_generic.
    std::string line(
        "{\"action\":\"A\",\"side\":\"B\","
        "\"hd\":{\"instrument_id\":12345,\"ts_event\":\"2026-04-01T07:00:00.000000000Z\","
        "\"rtype\":160,\"publisher_id\":42},"
        "\"ts_recv\":\"2026-04-01T07:00:00.123456789Z\","
        "\"price\":\"0.001234567\",\"size\":100,"
        "\"channel_id\":79,\"order_id\":\"9876543210\","
        "\"flags\":0,\"ts_in_delta\":1234,\"sequence\":555,"
        "\"symbol\":\"FOO\"}\n");
    const char* p = line.data();
    MarketDataEvent evt;
    fastparse::ParseContext ctx;
    EXPECT(fastparse::parse_line(p, line.data() + line.size(), evt, &ctx) != nullptr);
    EXPECT_EQ(evt.instrument_id, 12345u);
    EXPECT_EQ(static_cast<char>(evt.action), 'A');
    EXPECT_EQ(static_cast<char>(evt.side),   'B');
    EXPECT_EQ(evt.size,     100ULL);
    EXPECT_EQ(evt.order_id, 9876543210ULL);
    EXPECT_EQ(evt.sequence, 555ULL);
    EXPECT(evt.has_price);
}

static void test_parse_drops_no_ts() {
    std::string line(
        "{\"hd\":{\"rtype\":160,\"publisher_id\":42,\"instrument_id\":1},"
        "\"action\":\"R\",\"side\":\"N\",\"price\":null,"
        "\"size\":0,\"channel_id\":79,\"order_id\":\"0\","
        "\"flags\":0,\"ts_in_delta\":0,\"sequence\":1,"
        "\"symbol\":\"X\"}\n");
    const char* p = line.data();
    MarketDataEvent evt;
    fastparse::ParseContext ctx;
    EXPECT(fastparse::parse_line(p, line.data() + line.size(), evt, &ctx) != nullptr);
    EXPECT_EQ(evt.key_ts_ns, 0ULL);
}

struct VecSource {
    std::vector<MarketDataEvent> events;
    size_t pos = 0;
    const MarketDataEvent* head = nullptr;
    uint64_t key = 0;
    uint64_t seq = 0;

    bool advance() noexcept {
        if (pos >= events.size()) return false;
        head = &events[pos++];
        key  = head->key_ts_ns;
        seq  = head->sequence;
        return true;
    }
};

static MarketDataEvent make_evt(uint64_t ts, uint64_t seq) {
    MarketDataEvent e{};
    e.key_ts_ns  = ts;
    e.ts_recv_ns = ts;
    e.sequence   = seq;
    return e;
}

static void test_tournament_merge() {
    VecSource a, b, c;
    a.events = { make_evt(10, 0), make_evt(40, 0), make_evt(70, 0) };
    b.events = { make_evt(20, 0), make_evt(30, 0), make_evt(80, 0) };
    c.events = { make_evt(50, 0), make_evt(60, 0), make_evt(90, 0) };

    TournamentTree<VecSource, 4> tree;
    std::vector<VecSource*> srcs = { &a, &b, &c };
    tree.init(srcs);

    std::vector<uint64_t> got;
    while (VecSource* w = tree.top()) {
        got.push_back(w->head->key_ts_ns);
        tree.pop_advance();
    }
    const std::vector<uint64_t> expected = { 10, 20, 30, 40, 50, 60, 70, 80, 90 };
    EXPECT_EQ(got.size(), expected.size());
    for (size_t i = 0; i < got.size() && i < expected.size(); ++i) {
        EXPECT_EQ(got[i], expected[i]);
    }
}

static void test_tournament_tiebreak_by_seq() {
    VecSource a, b;
    a.events = { make_evt(10, 5), make_evt(10, 7) };
    b.events = { make_evt(10, 6), make_evt(10, 8) };

    TournamentTree<VecSource, 2> tree;
    std::vector<VecSource*> srcs = { &a, &b };
    tree.init(srcs);

    std::vector<uint64_t> seqs;
    while (VecSource* w = tree.top()) {
        seqs.push_back(w->head->sequence);
        tree.pop_advance();
    }
    const std::vector<uint64_t> expected = { 5, 6, 7, 8 };
    EXPECT_EQ(seqs.size(), expected.size());
    for (size_t i = 0; i < seqs.size() && i < expected.size(); ++i) {
        EXPECT_EQ(seqs[i], expected[i]);
    }
}

static void test_spsc_roundtrip() {
    using Ring = BatchedSpscRing<32, 4>;
    auto ring = std::make_unique<Ring>();
    constexpr size_t N = 10000;

    std::thread producer([&]{
        BatchProducer<Ring> emitter(ring.get());
        for (size_t i = 0; i < N; ++i) {
            MarketDataEvent& slot = emitter.next();
            slot = MarketDataEvent{};
            slot.sequence  = i;
            slot.key_ts_ns = 1000 + i;
            emitter.commit_one();
        }
        emitter.flush();
        ring->close();
    });

    BatchConsumer<Ring> consumer(ring.get());
    size_t i = 0;
    bool ordered = true;
    MarketDataEvent evt;
    while (consumer.pop(evt)) {
        if (evt.sequence != i) ordered = false;
        ++i;
    }
    producer.join();
    EXPECT_EQ(i, N);
    EXPECT(ordered);
}

static void test_chunk_file_empty() {
    auto chunks = chunk_file(nullptr, 0, 1024);
    EXPECT_EQ(chunks.size(), 0u);
}

static void test_chunk_file_smaller_than_target() {
    const char* data = "line1\nline2\nline3\n";
    const size_t n   = std::strlen(data);
    auto chunks = chunk_file(data, n, 1024);
    EXPECT_EQ(chunks.size(), 1u);
    if (!chunks.empty()) {
        EXPECT(chunks[0].begin == data);
        EXPECT(chunks[0].end   == data + n);
    }
}

static void test_chunk_file_no_trailing_newline() {
    // Last chunk falls off the end without a '\n'; still must cover [base, end).
    const char* data = "ab\ncd\nef";  // 8 bytes, no trailing newline
    const size_t n   = std::strlen(data);
    auto chunks = chunk_file(data, n, 3);
    EXPECT(chunks.size() >= 1u);
    if (!chunks.empty()) {
        EXPECT(chunks.front().begin == data);
        EXPECT(chunks.back().end    == data + n);
    }
    // Every internal split must be right after a '\n'.
    for (size_t i = 1; i < chunks.size(); ++i) {
        const char* prev_end = chunks[i - 1].end;
        EXPECT(prev_end > data && *(prev_end - 1) == '\n');
        EXPECT(chunks[i].begin == prev_end);
    }
}

static void test_chunk_file_multi_chunk_line_aligned() {
    // Build something deterministic: 6 lines, each 10 bytes incl. '\n'.
    std::string buf;
    for (int i = 0; i < 6; ++i) buf += "abcdefghi\n";   // 10 * 6 = 60
    const size_t n = buf.size();
    auto chunks = chunk_file(buf.data(), n, 15);  // forces multiple chunks

    EXPECT(chunks.size() > 1u);
    if (chunks.empty()) return;

    // Coverage: chunks tile [base, base+n) exactly, no gaps, no overlap.
    EXPECT(chunks.front().begin == buf.data());
    EXPECT(chunks.back().end    == buf.data() + n);
    for (size_t i = 1; i < chunks.size(); ++i) {
        EXPECT(chunks[i].begin == chunks[i - 1].end);
    }
    // Line-alignment: every internal split sits just past a '\n'.
    for (size_t i = 0; i + 1 < chunks.size(); ++i) {
        const char* end = chunks[i].end;
        EXPECT(end > buf.data() && *(end - 1) == '\n');
    }
}

static void test_chunk_file_target_exactly_size() {
    const char* data = "abc\ndef\n";
    const size_t n   = std::strlen(data);
    auto chunks = chunk_file(data, n, n);   // size == target → single chunk
    EXPECT_EQ(chunks.size(), 1u);
    if (!chunks.empty()) EXPECT(chunks[0].end == data + n);
}

// Mirror of merger_node_main from main_hard.cpp, scoped to the test so
// the multi-level hierarchy is exercised end-to-end without depending
// on `static` symbols inside that translation unit.
template <size_t MaxLeaves, typename Ring>
static void run_merger_node(std::vector<Ring*> inputs, Ring* output) {
    struct LocalSource {
        BatchConsumer<Ring> consumer;
        const MarketDataEvent* head = nullptr;
        uint64_t key = 0;
        uint64_t seq = 0;
        explicit LocalSource(Ring* r) noexcept : consumer(r) {}
        bool advance() noexcept {
            head = consumer.peek();
            if (!head) return false;
            key = head->key_ts_ns;
            seq = head->sequence;
            return true;
        }
    };
    std::vector<std::unique_ptr<LocalSource>> srcs;
    srcs.reserve(inputs.size());
    for (auto* q : inputs) srcs.push_back(std::make_unique<LocalSource>(q));
    std::vector<LocalSource*> raw;
    raw.reserve(srcs.size());
    for (auto& s : srcs) raw.push_back(s.get());

    TournamentTree<LocalSource, MaxLeaves> tree;
    tree.init(raw);

    BatchProducer<Ring> emitter(output);
    while (LocalSource* w = tree.top()) {
        emitter.emit(*w->head);
        tree.pop_advance();
    }
    emitter.flush();
    output->close();
}

static void test_hierarchy_two_level_merge() {
    using Ring = BatchedSpscRing<32, 4>;

    // Four leaves of pre-sorted events. Interleaved so every internal
    // node sees real reordering work, not a trivial concat.
    std::vector<std::vector<uint64_t>> leaves = {
        { 10, 40, 90 },
        { 20, 50, 100 },
        { 30, 60, 110 },
        { 70, 80, 120 },
    };
    constexpr size_t L = 4;

    std::vector<std::unique_ptr<Ring>> leaf_qs;
    leaf_qs.reserve(L);
    for (size_t i = 0; i < L; ++i) leaf_qs.emplace_back(std::make_unique<Ring>());

    // Producers for each leaf.
    std::vector<std::thread> producers;
    producers.reserve(L);
    for (size_t i = 0; i < L; ++i) {
        Ring* ring = leaf_qs[i].get();
        producers.emplace_back([ring, evs = leaves[i]]{
            BatchProducer<Ring> emitter(ring);
            uint64_t seq = 0;
            for (uint64_t ts : evs) {
                MarketDataEvent& slot = emitter.next();
                slot = MarketDataEvent{};
                slot.key_ts_ns  = ts;
                slot.ts_recv_ns = ts;
                slot.sequence   = seq++;
                emitter.commit_one();
            }
            emitter.flush();
            ring->close();
        });
    }

    // Level 1: two internal mergers (2 leaves each).
    auto m1_q = std::make_unique<Ring>();
    auto m2_q = std::make_unique<Ring>();
    std::thread m1(run_merger_node<2, Ring>,
                   std::vector<Ring*>{ leaf_qs[0].get(), leaf_qs[1].get() },
                   m1_q.get());
    std::thread m2(run_merger_node<2, Ring>,
                   std::vector<Ring*>{ leaf_qs[2].get(), leaf_qs[3].get() },
                   m2_q.get());

    // Root merger.
    auto root_q = std::make_unique<Ring>();
    std::thread root(run_merger_node<2, Ring>,
                     std::vector<Ring*>{ m1_q.get(), m2_q.get() },
                     root_q.get());

    // Drain and verify.
    BatchConsumer<Ring> consumer(root_q.get());
    std::vector<uint64_t> got;
    MarketDataEvent evt;
    while (consumer.pop(evt)) got.push_back(evt.key_ts_ns);

    for (auto& t : producers) t.join();
    m1.join(); m2.join(); root.join();

    const std::vector<uint64_t> expected = {
        10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120
    };
    EXPECT_EQ(got.size(), expected.size());
    for (size_t i = 0; i < got.size() && i < expected.size(); ++i) {
        EXPECT_EQ(got[i], expected[i]);
    }
}

int main() {
    test_parse_positional_golden();
    test_parse_undef_price();
    test_parse_null_price();
    test_parse_generic_reordered();
    test_parse_drops_no_ts();
    test_tournament_merge();
    test_tournament_tiebreak_by_seq();
    test_spsc_roundtrip();
    test_chunk_file_empty();
    test_chunk_file_smaller_than_target();
    test_chunk_file_no_trailing_newline();
    test_chunk_file_multi_chunk_line_aligned();
    test_chunk_file_target_exactly_size();
    test_hierarchy_two_level_merge();

    std::cout << "passed: " << g_passed << ", failed: " << g_failed << "\n";
    return g_failed == 0 ? 0 : 1;
}
