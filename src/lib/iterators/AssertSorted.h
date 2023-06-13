#pragma once

#include "Iterator.h"

/**
 * Checks if the input is ascending and therefore sorted.
 */
class AssertSorted : public Iterator {
private:
    Iterator *input;
    Row prev;
    bool is_sorted;
    size_t num_rows;

public:
    explicit AssertSorted(Iterator *iterator);

    ~AssertSorted();

    bool isSorted() const;

    size_t count() const;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;
};