#pragma once

#include "Iterator.h"
#include "../defs.h"

#include <vector>
#include <random>

namespace ovc::iterators {

    class Generator : public IGenerator {
    public :
        explicit Generator(Count rows, unsigned long upper, unsigned long seed = -1, bool store = false);

        Row *next() override;

        std::vector<Row> rows;

        IGenerator *clone() const override {
            return new Generator(num_rows, upper_, seed_, store);
        }

    private :
        unsigned long num_rows;
        unsigned long tid;
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        bool store;
        unsigned long upper_;
        unsigned long seed_;
    };
}