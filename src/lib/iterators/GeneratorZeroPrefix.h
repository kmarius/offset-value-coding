#pragma once

#include "Iterator.h"

#include <vector>
#include <random>

class GeneratorZeroPrefix : public Iterator {
public :
    explicit GeneratorZeroPrefix(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

    Row *next() override;

    std::vector<Row> rows;

private :
    unsigned long num_rows;
    int prefix;
    int upper;
    std::mt19937 rng;
    std::uniform_int_distribution<std::mt19937::result_type> dist;
    Row buf;
};