// Tournament tree for k-way merge. tree_[1] holds the winning leaf;
// pop_advance() replays only the root-bound path.
//
// Source contract: advance() returns false when drained; while live,
// expose head (event ptr), key (head->key_ts_ns), seq (head->sequence).
// best() reads key/seq only — never dereferences head on the hot path.
//
// MaxLeaves: power of two ≥ 2. Surplus slots are nullptr and always lose.
#pragma once

#include "../../common/event.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ingest {

template <typename Source, size_t MaxLeaves>
class TournamentTree {
    static_assert(MaxLeaves >= 2 && (MaxLeaves & (MaxLeaves - 1)) == 0);

public:
    void init(const std::vector<Source*>& srcs) noexcept {
        for (size_t i = 0; i < MaxLeaves; ++i) {
            Source* s = (i < srcs.size()) ? srcs[i] : nullptr;
            if (s && !s->advance()) s = nullptr;
            leaves_[i] = s;
        }
        for (size_t i = MaxLeaves - 1; i >= 1; --i) {
            tree_[i] = best(child_winner(2 * i), child_winner(2 * i + 1));
        }
    }

    Source* top() const noexcept {
        return leaves_[tree_[1]];
    }

    void pop_advance() noexcept {
        const size_t w = tree_[1];
        if (!leaves_[w]->advance()) leaves_[w] = nullptr;
        size_t i = (w + MaxLeaves) >> 1;
        while (i >= 1) {
            tree_[i] = best(child_winner(2 * i), child_winner(2 * i + 1));
            i >>= 1;
        }
    }

private:
    Source* leaves_[MaxLeaves]{};
    // tree_[1] = root. Internal nodes 1..MaxLeaves-1; leaf index n at
    // node MaxLeaves+n via child_winner().
    size_t  tree_[MaxLeaves]{};

    size_t child_winner(size_t node) const noexcept {
        return (node >= MaxLeaves) ? (node - MaxLeaves) : tree_[node];
    }

    size_t best(size_t a, size_t b) const noexcept {
        const Source* sa = leaves_[a];
        const Source* sb = leaves_[b];
        if (!sa) return b;
        if (!sb) return a;
        if (sa->key != sb->key) return (sa->key < sb->key) ? a : b;
        return (sa->seq < sb->seq) ? a : b;
    }
};

} // namespace ingest
