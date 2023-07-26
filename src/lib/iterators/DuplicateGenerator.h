#pragma once

#include <unordered_set>
#include "Iterator.h"

namespace ovc::iterators {
    class DuplicateGenerator : public Generator {
    public:
        void open() override {
            Iterator::open();
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

    private:
        unsigned long num_rows;
        unsigned long count;
        unsigned long mult;
        unsigned long ind;
        std::vector<Row> rows;
    };
}