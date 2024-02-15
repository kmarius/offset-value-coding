#pragma once

#include "Iterator.h"
#include "RandomGenerator.h"

#include <vector>
#include <random>

namespace ovc::iterators {

    class RowGenerator : public RandomGenerator {
    public :
        RowGenerator(unsigned long num_rows, const uint8_t bits[ROW_ARITY], unsigned long seed = -1);
        RowGenerator(unsigned long num_rows, std::initializer_list<uint8_t> bits, unsigned long seed = -1);
        RowGenerator(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

        Row *next() override;

        std::vector<Row> rows;

        RowGenerator *clone() const {
            return new RowGenerator(num_rows, bits, seed);
        }

    private :
        RowGenerator(unsigned long num_rows, unsigned long seed, void *dummy);
        unsigned long num_rows;
        uint8_t bits[ROW_ARITY]; // bits used per column
        Row buf;
    };
}