#include "GeneratorZeroPrefix.h"

GeneratorZeroPrefix::GeneratorZeroPrefix(unsigned long num_rows, int upper, int prefix, unsigned long seed)
: IGenerator(), num_rows(num_rows), upper(upper), prefix(prefix), buf({0}), seed_(0) {
    std::random_device dev;
    seed = seed == (unsigned long) -1 ? dev() : seed;
    seed_ = seed;
    rng = std::mt19937(seed);
    dist = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
}

Row *GeneratorZeroPrefix::next() {
    Iterator::next();
    if (num_rows == 0) {
        return nullptr;
    }
    num_rows--;

    for (int i = prefix; i < ROW_ARITY; i++) {
        buf.columns[i] = dist(rng) % upper;
        buf.tid++;
    }
    return &buf;
}