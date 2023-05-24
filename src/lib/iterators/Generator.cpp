#include "Generator.h"
#include "lib/log.h"

#include <random>

Generator::Generator(Count num_rows, unsigned long upper, unsigned long seed)
        : num_rows(num_rows), tid(0) {
    std::random_device dev;
    seed = seed == (unsigned long) -1 ? dev() : seed;
    rng = std::mt19937(seed);
    dist = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
    log_info("using seed %lu", seed);
}

Generator::~Generator() = default;

void Generator::open() {
    assert(status == Unopened);
    status = Opened;
}

void Generator::close() {
    assert(status == Opened);
    status = Closed;
}

Row *Generator::next() {
    assert(status == Opened);
    if (num_rows == 0)
        return nullptr;
    num_rows--;
    buf = {0, tid++, {dist(rng), dist(rng), dist(rng), dist(rng)}};
    return &buf;
}

void Generator::free() {}