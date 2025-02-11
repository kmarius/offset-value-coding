#pragma once

#include "Iterator.h"
#include "RandomGenerator.h"

#include <vector>
#include <random>

namespace ovc::iterators {
    class GeneratorWithDomains : public RandomGenerator {
    public :
        GeneratorWithDomains(unsigned long num_rows, const uint64_t domains[ROW_ARITY], unsigned long seed = -1);
        GeneratorWithDomains(unsigned long num_rows, std::initializer_list<uint64_t> domains, unsigned long seed = -1);
        GeneratorWithDomains(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

        Row *next() override;

        std::vector<Row> rows;

        GeneratorWithDomains *clone() const {
            return new GeneratorWithDomains(num_rows, domains, seed);
        }

    private :
        GeneratorWithDomains(unsigned long num_rows, unsigned long seed, void *dummy);
        unsigned long num_rows;
        uint64_t domains[ROW_ARITY];
        Row buf;
    };
}