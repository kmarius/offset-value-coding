#pragma once

#include <chrono>

namespace ovc {
    template<
            class result_t   = std::chrono::milliseconds,
            class clock_t    = std::chrono::steady_clock,
            class duration_t = std::chrono::milliseconds
    >
    static inline auto since(std::chrono::time_point<clock_t, duration_t> const &start) {
        return std::chrono::duration_cast<result_t>(clock_t::now() - start).count();
    }

    static inline auto now() {
        return std::chrono::steady_clock::now();
    }

    static inline bool is_aligned(const void *ptr, std::uintptr_t alignment) noexcept {
        auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
        return iptr % alignment == 0;
    }
}