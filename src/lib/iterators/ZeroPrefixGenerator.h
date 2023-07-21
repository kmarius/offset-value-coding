#pragma once

#include "Iterator.h"

#include <vector>
#include <random>

namespace ovc::iterators {

    class ZeroPrefixGenerator : public Generator {
    public :
        explicit ZeroPrefixGenerator(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

        Row *next() override;

        std::vector<Row> rows;

        ZeroPrefixGenerator *clone() const {
            return new ZeroPrefixGenerator(num_rows, upper, prefix, seed_);
        }

    private :
        unsigned long num_rows;
        int prefix;
        int upper;
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        unsigned long seed_;
    };
}