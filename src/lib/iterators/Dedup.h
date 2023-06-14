#pragma once

#include "Iterator.h"

class Dedup : public Iterator {
    Iterator *input_;
    Row prev;
    bool has_prev;
public:
    explicit Dedup(Iterator *input);

    ~Dedup() override;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;

    bool outputIsSorted() override {
        return true;
    }

    size_t num_dupes;
};