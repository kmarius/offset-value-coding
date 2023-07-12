#pragma once

#include "UnaryIterator.h"

namespace ovc::iterators {

    class PrefixTruncationCounter : public UnaryIterator {
    public:
        explicit PrefixTruncationCounter(Iterator *input);

        Row *next() override;

        unsigned long getColumnComparisons() const;

    private:
        unsigned long count;
        Row prev;
        bool has_prev;
    };
}