#pragma once

#include "Iterator.h"

#include <vector>
#include <random>

class Generator : public Iterator {
public :
    explicit Generator(Count rows, unsigned long upper, unsigned long seed = -1, bool store = false);

    ~Generator() override;

    void open() override;

    void close() override;

    Row *next() override;

    void free() override;

    std::vector<Row> rows;

private :
    unsigned long num_rows;
    unsigned long tid;
    std::mt19937 rng;
    std::uniform_int_distribution<std::mt19937::result_type> dist;
    Row buf;
    bool store;
};
