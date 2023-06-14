#include "Generator.h"
#include "lib/log.h"

#include <random>

Generator::Generator(Count num_rows, unsigned long upper, unsigned long seed, bool store)
        : num_rows(num_rows), tid(0), store(store) {
    std::random_device dev;
    seed = seed == (unsigned long) -1 ? dev() : seed;
    rng = std::mt19937(seed);
    dist = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
    log_info("using seed %lu", seed);
}

Row *Generator::next() {
    Iterator::next();

    if (num_rows == 0) {
        return nullptr;
    }
    num_rows--;
    //buf = {0, tid++, {dist(rng) % 2, dist(rng) % 4, dist(rng) % 8, dist(rng) % 16}};
    buf = {0, tid++, {dist(rng) % 1, dist(rng) % 2, dist(rng) % 4, dist(rng) % 8, dist(rng) % 8, dist(rng) % 8, dist(rng) % 8, dist(rng) % 8}};
    if (store) {
        rows.push_back(buf);
    }
    return &buf;
}