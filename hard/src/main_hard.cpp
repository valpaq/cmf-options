// Hard task: parse a folder of MBO NDJSON, merge into one chronological
// stream via flat or hierarchy, dispatch to processMarketDataEvent.
//
//   ./ingest_hard <folder> [flat|hierarchy|both]   (default: both)
//
// `both` mode FNV-1a-hashes each run; mismatched hashes ⇒ different order.

#include "../../common/parse.h"
#include "../../common/fast_parse.h"
#include "chunk.h"
#include "spsc_ring.h"
#include "tournament.h"

#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#if defined(__APPLE__)
#include <pthread.h>
#include <sys/qos.h>
#endif

using namespace ingest;
namespace fs = std::filesystem;

// Higher QoS biases toward P-cluster. Mergers/consumer want P-cores
// (in-cluster cache transfers); producers tolerate E-cores.
static inline void set_qos(int qclass) noexcept {
#if defined(__APPLE__)
    pthread_set_qos_class_self_np(static_cast<qos_class_t>(qclass), 0);
#else
    (void)qclass;
#endif
}

// 8192 × 64 ≈ 524 K events per edge. See perf_journey.md.
using EventQueue = BatchedSpscRing<8192, 64>;

// ~30 producers (22 files + chunks); 64 fits the tree in a few cache lines.
constexpr size_t kMaxLeaves = 64;

struct ProducerStats {
    uint64_t produced          = 0;
    uint64_t parse_errors      = 0;
    uint64_t dropped_no_ts     = 0;
    uint64_t fallback_ts_event = 0;
    uint64_t bad_ts_recv       = 0;
    uint64_t maybe_bad_book    = 0;
};

// Range must be line-aligned (chunk_file() handles that). Events without
// a usable timestamp are dropped.
static void producer_main(const char* begin, const char* end,
                          EventQueue& out,
                          ProducerStats* stats) {
#if defined(__APPLE__)
    set_qos(QOS_CLASS_USER_INITIATED);
#endif
    fastparse::ParseContext ctx;
    BatchProducer<EventQueue> emitter(&out);
    const char* p = begin;

    while (p < end) {
        const char* saved = p;
        MarketDataEvent& slot = emitter.next();
        const char* next = fastparse::parse_line(p, end, slot, &ctx);
        if (!next) {
            ++stats->parse_errors;
            p = saved + 1;
            continue;
        }

        const uint64_t key = slot.key_ts_ns;
        if (key == 0 || key == UNDEF_TIMESTAMP) {
            ++stats->dropped_no_ts;
            continue;
        }
        if (slot.ts_recv_ns == 0)            ++stats->fallback_ts_event;
        if (slot.flags & F_BAD_TS_RECV)      ++stats->bad_ts_recv;
        if (slot.flags & F_MAYBE_BAD_BOOK)   ++stats->maybe_bad_book;

        emitter.commit_one();
        ++stats->produced;
    }
    emitter.flush();
    out.close();
}

// Tournament-tree leaf. Caches comparator fields (key, seq) locally so
// best() never dereferences a producer-owned cache line on the hot path.
struct Source {
    BatchConsumer<EventQueue> consumer;
    const MarketDataEvent* head = nullptr;
    uint64_t key = 0;
    uint64_t seq = 0;

    explicit Source(EventQueue* ring) noexcept : consumer(ring) {}

    bool advance() noexcept {
        head = consumer.peek();
        if (!head) return false;
        key = head->key_ts_ns;
        seq = head->sequence;
        return true;
    }
};

// Flat: kMaxLeaves=64. Hierarchy internal nodes use 2/4/8 to shrink
// pop_advance() depth.
template <size_t MaxLeaves, typename Emit>
static uint64_t kway_merge(std::vector<std::unique_ptr<Source>>& srcs,
                           Emit&& sink_emit) {
    std::vector<Source*> raw;
    raw.reserve(srcs.size());
    for (auto& s : srcs) raw.push_back(s.get());

    TournamentTree<Source, MaxLeaves> tree;
    tree.init(raw);

    uint64_t merged = 0;
    while (Source* w = tree.top()) {
        sink_emit(*w->head);
        ++merged;
        tree.pop_advance();
    }
    return merged;
}

// Default 192 MiB: largest file (~500 MB) splits to ~3 chunks; smaller
// files stay as one. Override via CHUNK_MB env.
static size_t chunk_target_bytes() {
    static const size_t v = []{
        const char* s = std::getenv("CHUNK_MB");
        size_t mb = (s && *s) ? std::strtoull(s, nullptr, 10) : 192;
        if (mb < 16)   mb = 16;
        if (mb > 4096) mb = 4096;
        return mb * 1024 * 1024;
    }();
    return v;
}

// Plain static (not thread_local) — main thread is the sole dispatcher;
// removes _tlv_get_addr (~3% of consumer samples).
static uint64_t print_every() {
    static const uint64_t v = []{
        const char* s = std::getenv("PRINT_EVERY");
        return (s && *s) ? std::strtoull(s, nullptr, 10) : 1'000'000ULL;
    }();
    return v ? v : 1;
}

struct LobBest {
    double best_bid = 0.0;
    double best_ask = 0.0;
};

static uint64_t g_event_count = 0;
static uint64_t g_event_hash  = 0xcbf29ce484222325ULL;
static uint64_t g_lob_updates = 0;
static std::unordered_map<uint32_t, LobBest> g_lob;
static bool     g_cross_check = true;

static void mode_reset() noexcept {
    g_event_count = 0;
    g_event_hash  = 0xcbf29ce484222325ULL;
    g_lob_updates = 0;
    g_lob.clear();
}

// One xor + one prime multiply per field. Byte-unrolled FNV ran 8× the
// work for no extra collision safety on these inputs.
static constexpr uint64_t kFnvPrime = 0x100000001b3ULL;
static inline uint64_t fnv_step(uint64_t h, uint64_t v) noexcept {
    return (h ^ v) * kFnvPrime;
}

// Toy LOB: running best bid/ask per instrument. Cancel/Fill not modelled.
static void lob_update(const MarketDataEvent& e) noexcept {
    if (!e.has_price) return;
    if (e.action != Action::Add && e.action != Action::Modify) return;
    auto& slot = g_lob[e.instrument_id];
    if (e.side == Side::Bid) {
        if (slot.best_bid == 0.0 || e.price > slot.best_bid) {
            slot.best_bid = e.price;
            ++g_lob_updates;
        }
    } else if (e.side == Side::Ask) {
        if (slot.best_ask == 0.0 || e.price < slot.best_ask) {
            slot.best_ask = e.price;
            ++g_lob_updates;
        }
    }
}

static void processMarketDataEvent(const MarketDataEvent& event) {
    const uint64_t n = ++g_event_count;

    if (g_cross_check) {
        uint64_t h = g_event_hash;
        h = fnv_step(h, event.key_ts_ns);
        h = fnv_step(h, event.ts_recv_ns);
        h = fnv_step(h, std::bit_cast<uint64_t>(event.price));
        h = fnv_step(h, event.size);
        h = fnv_step(h, event.sequence);
        h = fnv_step(h, event.order_id);
        h = fnv_step(h, (static_cast<uint64_t>(event.instrument_id) << 32)
                      |  static_cast<uint64_t>(event.flags));
        h = fnv_step(h, (static_cast<uint64_t>(static_cast<uint8_t>(event.action))      <<  0)
                      | (static_cast<uint64_t>(static_cast<uint8_t>(event.side))        <<  8)
                      | (static_cast<uint64_t>(event.has_price ? 1u : 0u)               << 16));
        g_event_hash = h;

        lob_update(event);
    }

    if (n % print_every() == 0) {
        event.print(std::cout);
    }
}

struct BenchResult {
    uint64_t total                = 0;
    double   wall                 = 0.0;
    uint64_t event_hash           = 0;
    uint64_t distinct_instruments = 0;
    uint64_t lob_updates          = 0;
    ProducerStats agg{};
};

// Stats and rings indexed in lockstep with chunks.
struct ProducerSetup {
    std::vector<std::unique_ptr<EventQueue>> rings;
    std::vector<std::thread>                 threads;
};
static ProducerSetup spawn_producers(const std::vector<ChunkSpec>& chunks,
                                     std::vector<ProducerStats>& stats) {
    ProducerSetup setup;
    setup.rings.reserve(chunks.size());
    setup.threads.reserve(chunks.size());
    for (size_t i = 0; i < chunks.size(); ++i)
        setup.rings.emplace_back(std::make_unique<EventQueue>());
    for (size_t i = 0; i < chunks.size(); ++i) {
        EventQueue* ring = setup.rings[i].get();
        setup.threads.emplace_back(producer_main,
                                   chunks[i].begin, chunks[i].end,
                                   std::ref(*ring),
                                   &stats[i]);
    }
    return setup;
}

static ProducerStats aggregate(const std::vector<ProducerStats>& v) {
    ProducerStats r{};
    for (const auto& s : v) {
        r.produced          += s.produced;
        r.parse_errors      += s.parse_errors;
        r.dropped_no_ts     += s.dropped_no_ts;
        r.fallback_ts_event += s.fallback_ts_event;
        r.bad_ts_recv       += s.bad_ts_recv;
        r.maybe_bad_book    += s.maybe_bad_book;
    }
    return r;
}

static BenchResult finalize(uint64_t merged, double wall,
                            const std::vector<ProducerStats>& stats) {
    return BenchResult{
        .total                = merged,
        .wall                 = wall,
        .event_hash           = g_event_hash,
        .distinct_instruments = g_lob.size(),
        .lob_updates          = g_lob_updates,
        .agg                  = aggregate(stats),
    };
}

static BenchResult run_flat(const std::vector<ChunkSpec>& chunks) {
    const size_t G = chunks.size();

    std::vector<ProducerStats> stats(G);
    auto t0 = std::chrono::high_resolution_clock::now();

    auto setup = spawn_producers(chunks, stats);
    auto& queues    = setup.rings;
    auto& producers = setup.threads;

    // Two-stage in cross-check mode: merger thread writes events into
    // final_ring, main thread consumes and dispatches. Splitting puts
    // merge and process on different cores. Single-mode keeps the merge
    // inline to skip one cross-cluster ring hop.
    uint64_t merged = 0;
    mode_reset();

    if (g_cross_check) {
        // Heap: 32 MiB ring would blow the 8 MB default pthread stack.
        auto final_ring = std::make_unique<EventQueue>();
        std::thread merger([&]{
            set_qos(QOS_CLASS_USER_INTERACTIVE);
            std::vector<std::unique_ptr<Source>> srcs;
            srcs.reserve(G);
            for (size_t i = 0; i < G; ++i)
                srcs.push_back(std::make_unique<Source>(queues[i].get()));
            BatchProducer<EventQueue> emitter(final_ring.get());
            merged = kway_merge<kMaxLeaves>(srcs, [&](const MarketDataEvent& e) {
                emitter.emit(e);
            });
            emitter.flush();
            final_ring->close();
        });
        BatchConsumer<EventQueue> consumer(final_ring.get());
        while (const MarketDataEvent* e = consumer.peek()) {
            processMarketDataEvent(*e);
        }
        merger.join();
    } else {
        std::vector<std::unique_ptr<Source>> srcs;
        srcs.reserve(G);
        for (size_t i = 0; i < G; ++i)
            srcs.push_back(std::make_unique<Source>(queues[i].get()));
        merged = kway_merge<kMaxLeaves>(srcs, [&](const MarketDataEvent& e) {
            processMarketDataEvent(e);
        });
    }

    for (auto& t : producers) t.join();
    auto t1 = std::chrono::high_resolution_clock::now();
    double wall = std::chrono::duration<double>(t1 - t0).count();

    return finalize(merged, wall, stats);
}

// Each internal node runs a small k-way merge. MaxLeaves is the smallest
// pow-of-2 ≥ inputs, so pop_advance() walks log2(MaxLeaves), not log2(64).
template <size_t MaxLeaves>
static void merger_node_main(std::vector<EventQueue*> inputs,
                             EventQueue* output) {
#if defined(__APPLE__)
    set_qos(QOS_CLASS_USER_INTERACTIVE);
#endif
    std::vector<std::unique_ptr<Source>> srcs;
    srcs.reserve(inputs.size());
    for (auto* q : inputs)
        srcs.push_back(std::make_unique<Source>(q));
    BatchProducer<EventQueue> emitter(output);
    kway_merge<MaxLeaves>(srcs, [&](const MarketDataEvent& e) {
        emitter.emit(e);
    });
    emitter.flush();
    output->close();
}

// Smallest power-of-two ≥ group_size, capped at 8.
using MergerFn = void(*)(std::vector<EventQueue*>, EventQueue*);
static MergerFn pick_merger(size_t group_size) noexcept {
    if (group_size <= 2) return &merger_node_main<2>;
    if (group_size <= 4) return &merger_node_main<4>;
    return &merger_node_main<8>;
}

static BenchResult run_hierarchy(const std::vector<ChunkSpec>& chunks) {
    const size_t N = chunks.size();

    mode_reset();
    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<ProducerStats> stats(N);
    auto setup = spawn_producers(chunks, stats);
    auto& leaf_qs   = setup.rings;
    auto& producers = setup.threads;

    // Fan-in 4: log4(30) ≈ 3 levels vs 5 for binary; halves merger threads.
    std::vector<EventQueue*> level;
    level.reserve(N);
    for (auto& q : leaf_qs) level.push_back(q.get());

    std::vector<std::unique_ptr<EventQueue>> internal_qs;
    std::vector<std::thread> mergers;

    static const size_t kFanIn = []{
        const char* s = std::getenv("FAN_IN");
        size_t v = (s && *s) ? std::strtoull(s, nullptr, 10) : 4;
        return (v < 2 || v > 8) ? 4 : v;
    }();
    const size_t fan = kFanIn;
    auto pick_group = [fan](size_t remaining) -> size_t {
        // ≥ fan+1 avoids leaving a remainder of 1.
        if (remaining >= fan + 1) return fan;
        if (remaining >= 2) return remaining;
        return 1;
    };

    while (level.size() > 1) {
        std::vector<EventQueue*> next;
        size_t i = 0;
        while (i < level.size()) {
            size_t group = pick_group(level.size() - i);
            if (group == 1) {
                next.push_back(level[i]);
                ++i;
                continue;
            }
            std::vector<EventQueue*> ins;
            ins.reserve(group);
            for (size_t j = 0; j < group; ++j) ins.push_back(level[i + j]);
            i += group;

            internal_qs.emplace_back(std::make_unique<EventQueue>());
            EventQueue* out_q = internal_qs.back().get();
            mergers.emplace_back(pick_merger(group), std::move(ins), out_q);
            next.push_back(out_q);
        }
        level.swap(next);
    }

    EventQueue* root = level.front();

    BatchConsumer<EventQueue> root_consumer(root);
    uint64_t merged = 0;
    MarketDataEvent evt;
    while (root_consumer.pop(evt)) {
        processMarketDataEvent(evt);
        ++merged;
    }

    for (auto& t : producers) t.join();
    for (auto& t : mergers)   t.join();

    auto t1 = std::chrono::high_resolution_clock::now();
    double wall = std::chrono::duration<double>(t1 - t0).count();
    return finalize(merged, wall, stats);
}

static bool is_mbo_file(const fs::path& path) {
    const auto name = path.filename().string();
    constexpr std::string_view kSuffix = ".mbo.json";
    return name.size() > kSuffix.size() && name.ends_with(kSuffix);
}

static std::vector<std::string> collect_files(const std::string& folder) {
    std::vector<std::string> out;
    for (const auto& e : fs::directory_iterator(folder)) {
        if (!e.is_regular_file()) continue;
        if (!is_mbo_file(e.path())) continue;
        out.push_back(e.path().string());
    }
    std::sort(out.begin(), out.end());
    return out;
}

static void print_bench(const char* label, const BenchResult& r) {
    std::cout << "\n=== " << label << " ===\n";
    std::cout << std::format("Total messages processed: {}\n", r.total);
    std::cout << std::format("Wall time:                {:.3f}s\n", r.wall);
    if (r.wall > 0)
        std::cout << std::format("Throughput:               {:.0f} msg/s\n",
                                 r.total / r.wall);
    std::cout << std::format("Parse errors:             {}\n", r.agg.parse_errors);
    std::cout << std::format("Dropped (no usable ts):   {}\n", r.agg.dropped_no_ts);
    std::cout << std::format("Used ts_event fallback:   {}\n", r.agg.fallback_ts_event);
    std::cout << std::format("F_BAD_TS_RECV (flag 8):   {}\n", r.agg.bad_ts_recv);
    std::cout << std::format("F_MAYBE_BAD_BOOK (flag 4):{}\n", r.agg.maybe_bad_book);
    std::cout << std::format("Distinct instruments:     {}\n", r.distinct_instruments);
    std::cout << std::format("LOB best-bid/ask updates: {}\n", r.lob_updates);
    std::cout << std::format("Event hash (FNV-1a):      {:#018x}\n", r.event_hash);
}

int main(int argc, char* argv[]) {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(nullptr);

    set_qos(QOS_CLASS_USER_INTERACTIVE);

    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <folder> [flat|hierarchy|both]\n";
        return 1;
    }
    const std::string folder = argv[1];
    const std::string mode   = (argc == 3) ? argv[2] : "both";

    auto files = collect_files(folder);
    if (files.empty()) {
        std::cerr << "No NDJSON files found in " << folder << "\n";
        return 1;
    }

    // mmap each file once; chunks share the page-fault cost.
    std::vector<MappedFile> mmaps(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        if (load_mmap(files[i].c_str(), mmaps[i]) != 0) {
            std::cerr << "Failed to mmap " << files[i] << "\n";
            return 1;
        }
    }

    const size_t target = chunk_target_bytes();
    std::vector<ChunkSpec> chunks;
    chunks.reserve(files.size() * 2);
    for (size_t i = 0; i < files.size(); ++i) {
        auto file_chunks = chunk_file(mmaps[i].base, mmaps[i].size, target);
        for (auto& c : file_chunks) chunks.push_back(c);
    }

    if (chunks.size() > kMaxLeaves) {
        std::cerr << "Got " << chunks.size() << " chunks; tournament tree is "
                     "sized for kMaxLeaves=" << kMaxLeaves
                  << ". Increase CHUNK_MB or bump kMaxLeaves.\n";
        return 1;
    }
    std::cerr << "Files: " << files.size() << " in " << folder
              << " (" << chunks.size() << " chunks @ "
              << (target >> 20) << " MiB target)\n";

    BenchResult flat{}, hier{};
    bool ran_flat = false, ran_hier = false;
    g_cross_check = (mode == "both");

    if (mode == "flat" || mode == "both") {
        flat = run_flat(chunks);
        print_bench("Flat merger", flat);
        ran_flat = true;
    }
    if (mode == "hierarchy" || mode == "both") {
        hier = run_hierarchy(chunks);
        print_bench("Hierarchy merger", hier);
        ran_hier = true;
    }

    if (ran_flat && ran_hier) {
        std::cout << "\n=== Cross-check (flat vs hierarchy) ===\n";
        const bool ok =
            flat.total                == hier.total &&
            flat.event_hash           == hier.event_hash &&
            flat.distinct_instruments == hier.distinct_instruments &&
            flat.lob_updates          == hier.lob_updates;
        std::cout << (ok ? "OK — flat and hierarchy emit byte-identical streams.\n"
                         : "MISMATCH — flat and hierarchy diverged.\n");
        if (!ok) return 2;
    }
    return 0;
}
