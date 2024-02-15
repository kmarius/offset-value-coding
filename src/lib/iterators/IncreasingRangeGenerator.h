#pragma once

#include "Iterator.h"
#include "../defs.h"
#include "RandomGenerator.h"

#include <vector>
#include <random>

namespace ovc::iterators {

    class IncreasingRangeGenerator : public RandomGenerator {
    public :
        explicit IncreasingRangeGenerator(Count rows, unsigned long upper, unsigned long seed = -1);

        Row *next() override;

        Generator *clone() const override {
            return new IncreasingRangeGenerator(num_rows, upper, seed);
        }

    private :
        unsigned long num_rows;
        unsigned long tid;
        Row buf;
        unsigned long upper;
    };
}