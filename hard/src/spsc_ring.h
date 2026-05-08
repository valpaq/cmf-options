// Batched SPSC ring. One writer, one reader. Slot count power-of-two.
// head_/tail_ and their cached counterparts each on their own cache line.
//
// Ordering:
//   writer: write batch → release head_.
//   reader: acquire head_ → read batch → release tail_.
//   close:  release closed_ AFTER the last writer commit. Reader treats
//           the stream as drained once closed_ is seen and head_ ≤ tail_.
#pragma once

#include "../../common/event.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

namespace ingest {

// Yield-only spin had 10× variance under oversubscription (33 threads on
// 12 cores). Ladder: pause → yield → 50 µs sleep.
struct RingBackoff {
    int n = 0;
    void operator()() noexcept {
        if (n < 32) {
#if defined(__aarch64__)
            asm volatile("yield" ::: "memory");
#elif defined(__x86_64__) || defined(__i386__)
            asm volatile("pause" ::: "memory");
#endif
        } else if (n < 96) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        ++n;
    }
    void reset() noexcept { n = 0; }
};

template <size_t kBatchSize_ = 128, size_t kSlotCount_ = 64>
class BatchedSpscRing {
    static_assert((kSlotCount_ & (kSlotCount_ - 1)) == 0);
    static constexpr size_t kMask = kSlotCount_ - 1;

public:
    static constexpr size_t kBatchSize = kBatchSize_;
    static constexpr size_t kSlotCount = kSlotCount_;

    struct Batch {
        std::array<MarketDataEvent, kBatchSize> events;
        uint32_t count = 0;
    };

    // Slot is valid until matching writer_commit(). Spins if full.
    Batch* writer_acquire() noexcept {
        const size_t h = head_.load(std::memory_order_relaxed);
        if (h - tail_cached_ >= kSlotCount) {
            RingBackoff bo;
            do {
                tail_cached_ = tail_.load(std::memory_order_acquire);
                if (h - tail_cached_ < kSlotCount) break;
                bo();
            } while (true);
        }
        Batch* b = &slots_[h & kMask];
        b->count = 0;
        return b;
    }

    void writer_commit() noexcept {
        head_.fetch_add(1, std::memory_order_release);
    }

    // nullptr iff closed AND drained. Spins while empty + open.
    Batch* reader_acquire() noexcept {
        const size_t t = tail_.load(std::memory_order_relaxed);
        if (t < head_cached_) return &slots_[t & kMask];
        head_cached_ = head_.load(std::memory_order_acquire);
        if (t < head_cached_) return &slots_[t & kMask];
        RingBackoff bo;
        while (true) {
            if (closed_.load(std::memory_order_acquire)) {
                head_cached_ = head_.load(std::memory_order_acquire);
                if (t < head_cached_) break;
                return nullptr;
            }
            bo();
            head_cached_ = head_.load(std::memory_order_acquire);
            if (t < head_cached_) break;
        }
        return &slots_[t & kMask];
    }

    void reader_release() noexcept {
        tail_.fetch_add(1, std::memory_order_release);
    }

    void close() noexcept {
        closed_.store(true, std::memory_order_release);
    }

private:
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) size_t tail_cached_ = 0;
    alignas(64) size_t head_cached_ = 0;
    alignas(64) std::atomic<bool> closed_{false};
    alignas(64) Batch slots_[kSlotCount];
};

// emit() copies; next()+commit_one()/discard() writes in place.
// Must flush() before closing or the tail is lost.
template <typename Ring>
class BatchProducer {
public:
    explicit BatchProducer(Ring* ring) noexcept : ring_(ring) {}

    BatchProducer(const BatchProducer&) = delete;
    BatchProducer& operator=(const BatchProducer&) = delete;

    // Slot is tentative until commit_one(); otherwise reused by next().
    MarketDataEvent& next() noexcept {
        if (!batch_) batch_ = ring_->writer_acquire();
        return batch_->events[batch_->count];
    }

    void commit_one() noexcept {
        ++batch_->count;
        if (batch_->count == Ring::kBatchSize) {
            ring_->writer_commit();
            batch_ = nullptr;
        }
    }

    void discard() noexcept {}

    void emit(const MarketDataEvent& e) noexcept {
        if (!batch_) batch_ = ring_->writer_acquire();
        batch_->events[batch_->count++] = e;
        if (batch_->count == Ring::kBatchSize) {
            ring_->writer_commit();
            batch_ = nullptr;
        }
    }

    void flush() noexcept {
        if (batch_ && batch_->count > 0) ring_->writer_commit();
        batch_ = nullptr;
    }

private:
    Ring* ring_;
    typename Ring::Batch* batch_ = nullptr;
};

// peek() returns a pointer valid only until the NEXT peek() (which may
// release the batch back to the producer). nullptr iff drained.
template <typename Ring>
class BatchConsumer {
public:
    explicit BatchConsumer(Ring* ring) noexcept : ring_(ring) {}

    BatchConsumer(const BatchConsumer&) = delete;
    BatchConsumer& operator=(const BatchConsumer&) = delete;

    bool pop(MarketDataEvent& out) noexcept {
        const MarketDataEvent* p = peek();
        if (!p) return false;
        out = *p;
        return true;
    }

    const MarketDataEvent* peek() noexcept {
        if (!batch_ || pos_ == batch_->count) {
            if (batch_) {
                ring_->reader_release();
                batch_ = nullptr;
            }
            batch_ = ring_->reader_acquire();
            if (!batch_) return nullptr;
            pos_ = 0;
        }
        return &batch_->events[pos_++];
    }

private:
    Ring* ring_;
    typename Ring::Batch* batch_ = nullptr;
    uint32_t pos_ = 0;
};

} // namespace ingest
