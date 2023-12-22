#pragma once

#include "Iterator.h"

#include <vector>
#include <random>
#include <set>

namespace ovc::iterators {
    class UniqueRowGenerator : public Generator {
    public :

        UniqueRowGenerator(unsigned long num_rows, int upper, int prefix, int arity, unsigned long seed = -1)
                : UniqueRowGenerator(num_rows, arity, seed, nullptr) {
            assert(arity <= ROW_ARITY);
            if (arity > ROW_ARITY) {
                arity = ROW_ARITY;
            }
            for (int i = 0; i < prefix; i++) {
                domains[i] = 1;
            }
            for (int i = prefix; i < arity && i < ROW_ARITY; i++) {
                domains[i] = upper;
            }
            for (int i = arity; i < ROW_ARITY; i++) {
                domains[i] = 1;
            }
        }

        UniqueRowGenerator(unsigned long num_rows, const uint64_t domains[ROW_ARITY], int arity,
                           unsigned long seed = -1)
                : UniqueRowGenerator(num_rows, arity, seed, nullptr) {
            memcpy(this->domains, domains, sizeof this->domains);
        }

        UniqueRowGenerator(unsigned long num_rows, std::initializer_list<uint64_t> domains,
                           unsigned long seed = -1)
                : UniqueRowGenerator(num_rows, (int) domains.size(), seed, nullptr) {
            int i = 0;
            for (auto b: domains) {
                if (i >= ROW_ARITY) {
                    break;
                }
                this->domains[i++] = b;
            }
            for (; i < ROW_ARITY; i++) {
                this->domains[i] = 1;
            }
        }

        UniqueRowGenerator *clone() const {
            return new UniqueRowGenerator(num_rows, domains, arity, seed);
        }

        Row *next() {
            Iterator::next();
            if (num_rows == 0) {
                return nullptr;
            }
            num_rows--;

            unsigned long hash;
            do {
                for (int i = 0; i < arity; i++) {
                    buf.columns[i] = dist(rng) % domains[i];
                }
                hash = buf.calcHash(arity);
            } while (hashes.find(hash) != hashes.end());

            buf.tid++;
            hashes.insert(hash);

            return &buf;
        }

    private :
        UniqueRowGenerator(unsigned long num_rows, int arity, unsigned long seed, void *dummy)
                : Generator(), num_rows(num_rows), buf({0}), domains(), arity(arity) {
            std::random_device dev;
            seed = seed == (unsigned long) -1 ? dev() : seed;
            this->seed = seed;
            rng = std::mt19937(seed);
            dist = std::uniform_int_distribution<std::mt19937::result_type>(0, UINT64_MAX);
        }

        int arity;
        unsigned long num_rows;
        uint64_t domains[ROW_ARITY]; // randomness per column
        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist;
        Row buf;
        unsigned long seed;
        std::set<unsigned long> hashes;
    };
}