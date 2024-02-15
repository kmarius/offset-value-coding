#pragma once

#include "Iterator.h"
#include "RandomGenerator.h"

#include <vector>
#include <random>

namespace ovc::iterators {
    class SegmentedGen : public RandomGenerator {
    public:
        SegmentedGen(unsigned long num_rows, const uint64_t domains[3], const uint8_t cols[3], unsigned long seed = -1)
                : RandomGenerator(seed), num_rows(num_rows), buf(), domains(), columns(), count(0) {
            memcpy(this->domains, domains, sizeof this->domains);
            memcpy(this->columns, cols, sizeof this->columns);
        }

        Row *next() override {
            Iterator::next();
            if (count >= num_rows) {
                return nullptr;
            }

            if (count && (count % (num_rows / domains[0])) == 0) {
                // increment segment
                buf.columns[columns[0]]++;
            }

            count++;

            if (domains[1])
                buf.columns[columns[1]] = dist(rng) % domains[1];
            if (domains[2])
                buf.columns[columns[2]] = dist(rng) % domains[2];

            buf.tid++;
            return &buf;
        }

        SegmentedGen *clone() const {
            return new SegmentedGen(num_rows, domains, columns, seed);
        }

    private:
        unsigned long num_rows;
        unsigned long count;
        uint64_t domains[3]; // bits used per column
        uint8_t columns[3]; // index of the column
        Row buf;
    };
}