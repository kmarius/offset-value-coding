#pragma once

#include "UnaryIterator.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    class DistinctBase : public UnaryIterator {
    public:
        explicit DistinctBase(Iterator *input);

        Row *next() override;

        size_t num_dupes;

    private:
        Row prev;
        bool has_prev;
    };

    typedef DistinctBase<true> Distinct;
    typedef DistinctBase<true> DistinctNoOvc;
}