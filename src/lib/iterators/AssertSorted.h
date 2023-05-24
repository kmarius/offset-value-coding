#pragma once

#include "Iterator.h"

class AssertSorted : public Iterator {
private:
    Iterator *input;
    Row prev_buf;
    Row *prev;
    bool sorted;
    size_t first_fail;
    size_t num_rows;
    Row fail0, fail1;

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
