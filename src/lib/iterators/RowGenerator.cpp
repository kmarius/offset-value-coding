#include <cstring>
#include "RowGenerator.h"

namespace ovc::iterators {

    RowGenerator::RowGenerator(unsigned long num_rows, unsigned long seed, void *dummy)
            : RandomGenerator(seed), num_rows(num_rows), buf({0}), bits() {}

    RowGenerator::RowGenerator(unsigned long num_rows, int upper, int prefix, unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        for (int i = prefix; i < ROW_ARITY; i++) {
            bits[i] = (uint8_t) (log(upper) / log(2));
        }
    }

    RowGenerator::RowGenerator(unsigned long num_rows, const uint8_t bits[ROW_ARITY], unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        memcpy(this->bits, bits, sizeof this->bits);
    }

    RowGenerator::RowGenerator(unsigned long num_rows, std::initializer_list<uint8_t> bits, unsigned long seed)
            : RowGenerator(num_rows, seed, nullptr) {
        int i = 0;
        for (auto b: bits) {
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