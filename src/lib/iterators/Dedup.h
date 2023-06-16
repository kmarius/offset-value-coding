#pragma once

#include "UnaryIterator.h"

class Dedup : public UnaryIterator {
public:
    explicit Dedup(Iterator *input);

    Row *next() override;

    bool outputIsSorted() override {
        return true;
    }

    size_t num_dupes;

private:
    Row prev;
    bool has_prev;
};