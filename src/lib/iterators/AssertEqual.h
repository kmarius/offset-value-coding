#pragma once

#include "BinaryIterator.h"

namespace ovc::iterators {

    class AssertEqual : public BinaryIterator {
    public:
        AssertEqual(Iterator *left, Iterator *right);

        void open() override;

        Row *next() override;

        void free() override;

        void close() override;

        bool equal;

    private:
        unsigned long count;
    };
}