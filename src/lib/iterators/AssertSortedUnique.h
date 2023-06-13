#pragma once

#include "Iterator.h"

/**
 * Checks if the input is strictly ascending and therefore sorted and uniqe.
 */
class AssertSortedUnique : public Iterator {
private:
    Iterator *input;
    Row prev;
    bool has_prev;
    bool sorted;
    size_t num_rows;

public:
    explicit AssertSortedUnique(Iterator *iterator);

    ~AssertSortedUnique() override;

    bool isSortedAndUnique() const;

    size_t count() const;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;
};