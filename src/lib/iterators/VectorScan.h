#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class VectorScan : public Iterator {
    public:
        explicit VectorScan(std::vector<Row> rows) : rows(std::move(rows)), index(0) {};

        Row *next() override {
            Iterator::next();
            if (index >= rows.size()) {
                return nullptr;
            }
            return &rows[index++];
        };

    private:
        std::vector<Row> rows;
        size_t index;
    };
}