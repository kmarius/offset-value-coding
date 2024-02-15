#include "IncreasingRangeGenerator.h"

namespace ovc::iterators {
    IncreasingRangeGenerator::IncreasingRangeGenerator(Count num_rows, unsigned long upper, unsigned long seed)
            : RandomGenerator(seed), num_rows(num_rows), tid(0), upper(upper) {}

    Row *IncreasingRangeGenerator::next() {
        Iterator::next();

        if (num_rows == 0) {
            return nullptr;
        }
        num_rows--;
        buf = {0, tid++,
               {
                       dist(rng) % 1,
                       dist(rng) % 2,
                       dist(rng) % 4,
                       dist(rng) % 8,
                       dist(rng) % 16,
                       dist(rng) % 32,
                       dist(rng) % 64,
                       dist(rng) % 100
               }
        };
        return &buf;
    }
}