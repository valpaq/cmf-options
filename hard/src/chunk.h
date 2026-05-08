// Line-aligned byte-range splitter for chunk-parallel parsing.
#pragma once

#include <cstddef>
#include <cstring>
#include <vector>

namespace ingest {

struct ChunkSpec {
    const char* begin = nullptr;
    const char* end   = nullptr;
};

// Splits into chunks ≈ target_bytes, each ending at '\n' (or end-of-input
// for the last). size ≤ target_bytes returns one chunk.
inline std::vector<ChunkSpec> chunk_file(const char* base, size_t size,
                                         size_t target_bytes) noexcept {
    std::vector<ChunkSpec> out;
    if (size == 0) return out;
    if (size <= target_bytes) {
        out.push_back({base, base + size});
        return out;
    }
    const char* end = base + size;
    const char* p   = base;
    while (p < end) {
        const char* tentative = p + target_bytes;
        if (tentative >= end) {
            out.push_back({p, end});
            break;
        }
        const void* nl = std::memchr(tentative, '\n',
                                     static_cast<size_t>(end - tentative));
        const char* split = nl ? static_cast<const char*>(nl) + 1 : end;
        out.push_back({p, split});
        p = split;
    }
    return out;
}

} // namespace ingest
