#include <cstring>
#include "GeneratorWithDomains.h"

namespace ovc::iterators {

    GeneratorWithDomains::GeneratorWithDomains(unsigned long num_rows, unsigned long seed, void *dummy)
            : RandomGenerator(seed), num_rows(num_rows), buf({0}), domains() {
        for (int i = 0; i < ROW_ARITY; i++) {
            domains[i] = 1;
        }
    }

    GeneratorWithDomains::GeneratorWithDomains(unsigned long num_rows, int upper, int prefix, unsigned long seed)
            : GeneratorWithDomains(num_rows, seed, nullptr) {
        for (int i = prefix; i < ROW_ARITY; i++) {
            domains[i] = upper;
        }
    }

    GeneratorWithDomains::GeneratorWithDomains(unsigned long num_rows, const uint64_t domains[ROW_ARITY], unsigned long seed)
            : GeneratorWithDomains(num_rows, seed, nullptr) {
        memcpy(this->domains, domains, sizeof this->domains);
    }

    GeneratorWithDomains::GeneratorWithDomains(unsigned long num_rows, std::initializer_list<uint64_t> domains, unsigned long seed)
            : GeneratorWithDomains(num_rows, seed, nullptr) {
        int i = 0;
        for (auto b : domains) {
            if (i >= ROW_ARITY) {
                break;
            }
            this->domains[i++] = b;
        }
    }

    Row *GeneratorWithDomains::next() {
        Iterator::next();
        if (num_rows == 0) {
            return nullptr;
        }
        num_rows--;

        for (int i = 0; i < ROW_ARITY; i++) {
            if (domains[i]) {
                buf.columns[i] = dist(rng) % domains[i];
            }
        }

        buf.tid++;
        return &buf;
    }
}