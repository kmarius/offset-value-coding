#pragma once

#include <random>
#include "Iterator.h"

namespace ovc::iterators {

    class ZeroSuffixGenerator : public Generator {
    public:
        explicit ZeroSuffixGenerator(unsigned long num_rows, int upper, int suffix = 0, unsigned long seed = -1);

        Row *next() override;

        std::vector<Row> rows;

        ZeroSuffixGenerator *clone() const override {
            return new ZeroSuffixGenerator(num_rows, upper, suffix, seed_);
        }

    private :
        unsigned long num_rows;
        int suffix;
        int upper;
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        unsigned long seed_;

    };
}