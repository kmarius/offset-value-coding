#pragma once

#include "UnaryIterator.h"
template<bool USE_OVC>
class DedupBase : public UnaryIterator {
public:
    explicit DedupBase(Iterator *input);

    Row *next() override;

    bool outputIsSorted() override {
        return true;
    }

    bool outputIsUnique() override {
        return true;
    }

    size_t num_dupes;

private:
    Row prev;
    bool has_prev;
};

typedef DedupBase<true> Dedup;
typedef DedupBase<true> DedupNoOvc;