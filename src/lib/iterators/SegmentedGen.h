#pragma once

#include "Iterator.h"

#include <vector>
#include <random>

namespace ovc::iterators {
    class SegmentedGen : public Generator {
    public:
        SegmentedGen(unsigned long num_rows, const uint64_t domains[3], const uint8_t cols[3],
                     unsigned long seed = -1)
                : SegmentedGen(num_rows, seed, nullptr) {
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

        std::vector<Row> rows;

        SegmentedGen *clone() const {
            return new SegmentedGen(num_rows, domains, columns, seed);
        }

    private :
        SegmentedGen(unsigned long num_rows, unsigned long seed, void *dummy)
                : Generator(), num_rows(num_rows), buf({0}), domains(), count(0) {
            std::random_device dev;
            seed = seed == (unsigned long) -1 ? dev() : seed;
            this->seed = seed;
            rng = std::mt19937(seed);
            dist = std::uniform_int_distribution<std::mt19937::result_type>(0, UINT64_MAX);
        }

        unsigned long count;
        unsigned long num_rows;
        uint64_t domains[3]; // bits used per column
        uint8_t columns[3]; // index of the column
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        unsigned long seed;
    };
}