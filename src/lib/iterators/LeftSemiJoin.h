#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class LeftSemiJoin : public BinaryIterator {
    public:
        LeftSemiJoin(Iterator *left, Iterator *right, unsigned join_columns);

        void open() override;

        Row *next() override;

        void free() override;

        void close() override;

    private:
        unsigned join_columns;
        std::vector<Row> buffer;
    };

}
