#pragma once

#include "Iterator.h"
#include "RowGenerator.h"

#include <vector>

namespace ovc::iterators {

    class DuplicateGenerator : public Generator {
    public:
        DuplicateGenerator(unsigned long num_rows, unsigned long mult, int prefix = 0, unsigned long seed = -1) : seed(seed),
                                                                                                  num_rows(num_rows),
                                                                                                  mult(mult), count(0), ind(0), rows(), prefix(prefix) {}

        void open() override {
            Iterator::open();
            rows = RowGenerator(num_rows / mult, 1 << 15, prefix, seed).collect();
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
            return new DuplicateGenerator(num_rows, mult, prefix, seed);
        }

    private:
        unsigned long seed;
        unsigned long num_rows;
        unsigned long count;
        unsigned long mult;
        unsigned long ind;
        int prefix;
        std::vector <Row> rows;
    };
}