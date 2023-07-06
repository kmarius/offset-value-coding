#include "GeneratorZeroSuffix.h"

GeneratorZeroSuffix::GeneratorZeroSuffix(unsigned long num_rows, int upper, int suffix, unsigned long seed)
    : IGenerator(), num_rows(num_rows), upper(upper), suffix(suffix), buf({0}), seed_(0) {
        std::random_device dev;
        seed = seed == (unsigned long) -1 ? dev() : seed;
        seed_ = seed;
        rng = std::mt19937(seed);
        dist = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
}


Row *GeneratorZeroSuffix::next() {
    Iterator::next();
    if (num_rows == 0) {
        return nullptr;
    }
    num_rows--;

    for (int i = 0; i < ROW_ARITY - suffix; i++) {
        buf.columns[i] = dist(rng) % upper;
        buf.tid++;
    }
    return &buf;
}