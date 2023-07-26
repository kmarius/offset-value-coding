#pragma once

#include "Iterator.h"

namespace ovc::iterators {
    class Multiplier : public UnaryIterator {
    public:
        Multiplier(Iterator *input, int mult) : UnaryIterator(input), dupes(0), mult(mult), row(nullptr) {}

        void open() override {
            Iterator::open();
            input_->open();
        }

        Row *next() override {
            Iterator::next();
            if (dupes++ % mult == 0) {
                row = input_->next();
            }
            return row;
        }

        void close() override {
            Iterator::close();
            input_->close();
        }
    private:
        int dupes;
        int mult;
        Row *row;
    };
}