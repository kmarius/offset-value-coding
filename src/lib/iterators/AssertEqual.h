#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class AssertEqual : public Iterator {
    public:
        AssertEqual(Iterator *left, Iterator *right);

        ~AssertEqual() override;

        void open() override;

        Row *next() override;

        void free() override;

        void close() override;

        bool equal;
    private:
        Iterator *left_;
        Iterator *right_;
        unsigned long count;
    };
}