#pragma once

#include "UnaryIterator.h"

namespace ovc::iterators {

/**
 * Checks if the input_ is strictly ascending and therefore sorted and uniqe.
 */
    class AssertSortedUnique : public UnaryIterator {
    public:
        explicit AssertSortedUnique(Iterator *input);

        bool isSortedAndUnique() const;

        size_t count() const;

        Row *next() override;

    private:
        Row prev;
        bool has_prev;
        bool is_sorted;
        size_t num_rows;
    };
}