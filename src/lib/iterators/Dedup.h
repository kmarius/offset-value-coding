#pragma once

#include "UnaryIterator.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    class DedupBase : public UnaryIterator {
    public:
        explicit DedupBase(Iterator *input);

        Row *next() override;

        size_t num_dupes;

    private:
        Row prev;
        bool has_prev;
    };

    typedef DedupBase<true> Dedup;
    typedef DedupBase<true> DedupNoOvc;
}