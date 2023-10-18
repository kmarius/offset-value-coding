#include "IncreasingRangeGenerator.h"
#include "lib/log.h"

#include <random>

namespace ovc::iterators {
    IncreasingRangeGenerator::IncreasingRangeGenerator(Count num_rows, unsigned long upper, unsigned long seed)
            : Generator(), num_rows(num_rows), tid(0), upper_(upper), seed_(seed) {
        std::random_device dev;
        seed = seed == (unsigned long) -1 ? dev() : seed;
        seed_ = seed;
        rng = std::mt19937(seed);
        dist = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
        log_info("using seed %lu", seed);
    }

    Row *IncreasingRangeGenerator::next() {
        Iterator::next();

        if (num_rows == 0) {
            return nullptr;
        }
        num_rows--;
        //buf = {0, tid++, {dist(rng) % 8, dist(rng) % 8, dist(rng) % 8, dist(rng) % 100, dist(rng) % 100, dist(rng) % 100, dist(rng) % 100, dist(rng) % 100}};
        buf = {0, tid++, {dist(rng) % 1, dist(rng) % 2, dist(rng) % 4, dist(rng) % 8, dist(rng) % 16, dist(rng) % 32,
                          dist(rng) % 64, dist(rng) % 100}};
        return &buf;
    }
}