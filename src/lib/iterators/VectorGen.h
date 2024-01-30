#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {

    // The first instance of the generator owns the data, so it must outlive all its clones.
    class VectorGen : public Generator {
        Row *rows;
        unsigned long length;
        unsigned long index;
        bool owns;

        explicit VectorGen(Row *rows, unsigned long length)
                : rows(rows), length(length), index(0), owns(false) {}

    public:
        explicit VectorGen(Iterator *input) : rows(nullptr), length(0), index(0), owns(true) {
            auto v = input->collect();
            length = v.size();
            rows = new Row[length];
            for (unsigned long i = 0; i < length; i++) {
                rows[i] = v[i];
            }
            delete input;
        }

        ~VectorGen() override {
            if (owns) {
                delete rows;
            }
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