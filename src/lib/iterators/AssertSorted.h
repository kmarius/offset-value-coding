#pragma once

#include "UnaryIterator.h"

/**
 * Checks if the input_ is ascending and therefore sorted.
 */
class AssertSorted : public UnaryIterator {
public:
    explicit AssertSorted(Iterator *iterator);

    bool isSorted() const;

    size_t count() const;

    Row *next() override;

private:
    Row prev;
    bool is_sorted;
    size_t num_rows;
};