#pragma once

#include "Iterator.h"

#include <vector>
#include <random>

namespace ovc::iterators {
    class RowGeneratorWithDomains : public Generator {
    public :
        RowGeneratorWithDomains(unsigned long num_rows, const uint64_t domains[ROW_ARITY], unsigned long seed = -1);
        RowGeneratorWithDomains(unsigned long num_rows, std::initializer_list<uint64_t> domains, unsigned long seed = -1);
        RowGeneratorWithDomains(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

        Row *next() override;

        std::vector<Row> rows;

        RowGeneratorWithDomains *clone() const {
            return new RowGeneratorWithDomains(num_rows, domains, seed);
        }

    private :
        RowGeneratorWithDomains(unsigned long num_rows, unsigned long seed, void *dummy);
        unsigned long num_rows;
        uint64_t domains[ROW_ARITY]; // bits used per column
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        unsigned long seed;
    };
}