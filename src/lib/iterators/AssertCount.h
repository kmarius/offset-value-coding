#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {

    class AssertCount : public UnaryIterator {
    public:
        AssertCount(Iterator *input) : UnaryIterator(input), count(0) {}

        void open() override {
            Iterator::open();
            input->open();
        };

        Row *next() override {
            Iterator::next();
            Row *row = input->next();
            if (!row) {
                return nullptr;
            }

            count++;
            return row;
        };

        void free() override {
            Iterator::free();
            input->free();
        };

        void close() override {
            Iterator::close();
            input->close();
        };

        unsigned long getCount() const {
            return count;
        }

    private:
        unsigned long count;
    };
}