#pragma once

#include "IGenerator.h"

#include <vector>
#include <random>

class GeneratorZeroPrefix : public IGenerator {
public :
    explicit GeneratorZeroPrefix(unsigned long num_rows, int upper, int prefix = 0, unsigned long seed = -1);

    Row *next() override;

    std::vector<Row> rows;

    GeneratorZeroPrefix *clone() const {
        return new GeneratorZeroPrefix(num_rows, upper, prefix, seed_);
    }

private :
    unsigned long num_rows;
    int prefix;
    int upper;
    std::mt19937 rng;
    std::uniform_int_distribution<std::mt19937::result_type> dist;
    Row buf;
    unsigned long seed_;
};