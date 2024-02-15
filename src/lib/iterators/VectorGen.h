#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {

    // The first instance of the generator owns the data, so it must outlive all its clones.
    class VectorGen : public Generator {
        Row *rows;
        unsigned long length;
        unsigned long index;

        explicit VectorGen(Row *rows, unsigned long length)
                : rows(new Row[length]), length(length), index(0) {
            memcpy(this->rows, rows, length * sizeof *rows);
        }

    public:
        explicit VectorGen(Iterator *input) : rows(nullptr), length(0), index(0) {
            auto vec = input->collect();
            length = vec.size();
            rows = new Row[length];
            memcpy(rows, &vec[0], length * sizeof *rows);
            delete input;
        }

        ~VectorGen() override {
            delete[] rows;
        };

        Row *next() override {
            Iterator::next();
            if (index >= length) {
                return nullptr;
            }
            return &rows[index++];
        }

        Generator *clone() const override {
            return new VectorGen(rows, length);
        };
    };
}