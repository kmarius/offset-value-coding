#pragma once

#include "Iterator.h"
#include "RowGenerator.h"
#include "UniqueRowGenerator.h"

#include <vector>

namespace ovc::iterators {

    class DuplicateGenerator : public Generator {
    public:
        DuplicateGenerator(unsigned long num_rows, int randomness, unsigned long mult, int prefix,
                           int arity, unsigned long seed) :
                seed(seed), num_rows(num_rows), mult(mult), count(0), ind(0), rows(), prefix(prefix), arity(arity),
                randomness(randomness) {}

        void open() override {
            Iterator::open();
            uint64_t domains[ROW_ARITY] = {0};
            int nonzero_columns = arity - prefix;
            int bits_per_column = randomness / nonzero_columns;
            for (unsigned long &domain: domains) {
                domain = 1;
            }
            for (int i = 0; i < nonzero_columns; i++) {
                domains[prefix + i] = 1 << bits_per_column;
                if (i < randomness % nonzero_columns) {
                    domains[prefix + i] <<= 1;
                }
            }
            rows = UniqueRowGenerator(num_rows / mult, domains, arity, seed).collect();
        }

        Row *next() override {
            Iterator::next();
            if (count >= num_rows) {
                return nullptr;
            }
            if (count++ % mult == 0) {
                return &rows[ind++];
            } else {
                unsigned long index = rand() % rows.size();
                return &rows[index];
            }
        }

        void close() override {
            Iterator::close();
            rows = {};
        }

        Generator *clone() const override {
            return new DuplicateGenerator(num_rows, randomness, mult, prefix, arity, seed);
        }

    private:
        unsigned long seed;
        unsigned long num_rows;
        unsigned long count;
        unsigned long mult;
        unsigned long ind;
        int randomness;
        int prefix;
        int arity;
        std::vector<Row> rows;
    };
}