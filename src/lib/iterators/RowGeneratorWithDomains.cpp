#include <cstring>
#include "RowGeneratorWithDomains.h"

namespace ovc::iterators {

    RowGeneratorWithDomains::RowGeneratorWithDomains(unsigned long num_rows, unsigned long seed, void *dummy)
            : Generator(), num_rows(num_rows), buf({0}), domains() {
        std::random_device dev;
        seed = seed == (unsigned long) -1 ? dev() : seed;
        this->seed = seed;
        rng = std::mt19937(seed);
        dist = std::uniform_int_distribution<std::mt19937::result_type>(0, UINT64_MAX);
    }

    RowGeneratorWithDomains::RowGeneratorWithDomains(unsigned long num_rows, int upper, int prefix, unsigned long seed)
            : RowGeneratorWithDomains(num_rows, seed, nullptr) {
        for (int i = prefix; i < ROW_ARITY; i++) {
            domains[i] = upper;
        }
    }

    RowGeneratorWithDomains::RowGeneratorWithDomains(unsigned long num_rows, const uint64_t domains[ROW_ARITY], unsigned long seed)
            : RowGeneratorWithDomains(num_rows, seed, nullptr) {
        memcpy(this->domains, domains, sizeof this->domains);
    }

    RowGeneratorWithDomains::RowGeneratorWithDomains(unsigned long num_rows, std::initializer_list<uint64_t> domains, unsigned long seed)
            : RowGeneratorWithDomains(num_rows, seed, nullptr) {
        int i = 0;
        for (auto b : domains) {
            if (i >= ROW_ARITY) {
                break;
            }
            this->domains[i++] = b;
        }
    }

    Row *RowGeneratorWithDomains::next() {
        Iterator::next();
        if (num_rows == 0) {
            return nullptr;
        }
        num_rows--;

        for (int i = 0; i < ROW_ARITY; i++) {
            buf.columns[i] = dist(rng) % domains[i];
        }

        buf.tid++;
        return &buf;
    }
}