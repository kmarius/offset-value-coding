#pragma once

#include "Iterator.h"

namespace ovc::iterators {
    class BinaryIterator : public Iterator {
    public:
        BinaryIterator(Iterator *left, Iterator *right) : left(left), right(right) {}

        ~BinaryIterator() override {
            delete left;
            delete right;
        }

    protected:
        Iterator *left;
        Iterator *right;
    };
}