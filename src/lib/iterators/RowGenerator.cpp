#include <cstring>
#include "RowGenerator.h"

namespace ovc::iterators {

    RowGenerator::RowGenerator(unsigned long num_rows, unsigned long seed, void *dummy)
            : Generator(), num_rows(num_rows), buf({0}), bits() {
        std::random_device dev;
        seed = seed == (unsigned long) -1 ? dev() : seed;
        this->seed = seed;
        rng = std::mt19937(seed);
        dist = std::uniform_int_distribution<std::mt19937::result_type>(0, UINT64_MAX);
    }

    RowGenerator::RowGenerator(unsigned long num_rows, int upper, int prefix, unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        for (int i = prefix; i < ROW_ARITY; i++) {
            bits[i] = (uint8_t) (log(upper) / log(2));
        }
    }

    RowGenerator::RowGenerator(unsigned long num_rows, const uint8_t bits[8], unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        memcpy(this->bits, bits, sizeof this->bits);
    }

    RowGenerator::RowGenerator(unsigned long num_rows, std::initializer_list<uint8_t> bits, unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        int i = 0;
        for (auto b : bits) {
            if (i >= ROW_ARITY) {
                break;
            }
            this->bits[i++] = b;
        }
    }

    Row *RowGenerator::next() {
        Iterator::next();
        if (num_rows == 0) {
            return nullptr;
        }
        num_rows--;

        auto rnd = dist(rng);
        for (int i = 0; i < ROW_ARITY; i++) {
            buf.columns[i] = rnd & ((1 << bits[i]) - 1);
            rnd >>= bits[i];
        }

        buf.tid++;
        return &buf;
    }
}