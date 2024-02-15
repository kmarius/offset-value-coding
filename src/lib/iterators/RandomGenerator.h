#pragma once

#include <random>
#include "Iterator.h"

namespace ovc::iterators {
    class RandomGenerator : public Generator {
    public:
        // seed of (uint64_t) -1 will generate a random seed instead
        explicit RandomGenerator(uint64_t seed)
                : dist(std::uniform_int_distribution<std::mt19937::result_type>(0, UINT64_MAX)),
                  seed(seed == (uint64_t) -1 ? std::random_device()() : seed) {
            rng = std::mt19937(this->seed);
        }

    protected:
        uint64_t seed;
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
    };
}