#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class PrefixTruncationCounter : public UnaryIterator {
    public:
        explicit PrefixTruncationCounter(Iterator *input);

        void open() override;

        void free() override;

        void close() override;

        Row *next() override;

        unsigned long getColumnComparisons() const;

    private:
        unsigned long count;
        Row prev;
        bool has_prev;
    };
}