#pragma once

#include "Iterator.h"

class Dedup : public Iterator {
    Iterator *input_;
public:
    explicit Dedup(Iterator *input);
    ~Dedup();

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;

    size_t num_dupes;
};
