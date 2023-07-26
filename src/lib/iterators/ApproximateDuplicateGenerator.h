#pragma once

#include <random>
#include <cmath>
#include "Iterator.h"
#include "lib/log.h"

namespace ovc::iterators {

    /* solve 0 = pow(1 - 1.0 / upper, num_rows) - (1 - 1.0 * output_size / upper) for upper using binary search */
    long long find_upper(int num_rows, int output_size) {
        unsigned long upper = 2;
        unsigned long step = 1;
        double cmp = std::pow(1 - 1.0 / upper, num_rows) - (1 - 1.0 * output_size / upper);
        while (cmp > 0) {
            step *= 2;
            upper += step;
            cmp = std::pow(1 - 1.0 / upper, num_rows) - (1 - 1.0 * output_size / upper);
        }
        while (step > 0) {
            cmp = pow(1 - 1.0 / upper, num_rows) - (1 - 1.0 * output_size / upper);
            upper += cmp > 0 ? step : -step;
            step /= 2;
        }
        return upper;
    }

    class ApproximateDuplicateGenerator : public Generator {

    public:

        ApproximateDuplicateGenerator(int num_rows, int output_size, int prefix = 0, int suffix = 0,
                                      unsigned long seed = -1)
                : num_rows(num_rows), output_size(output_size), prefix(prefix), suffix(suffix), row(), count(0) {

            std::random_device dev;
            this->seed = seed == (unsigned long) -1 ? dev() : seed;
            rng = std::mt19937(this->seed);

            int non_zero_columns = ROW_ARITY - prefix - suffix;
            assert(non_zero_columns >= 0);
            assert(num_rows >= output_size);

            long long global_upper = find_upper(num_rows, output_size);
            long long local_upper = std::pow(global_upper, 1.0 / non_zero_columns);

            long long acc = 1;
            auto rem = global_upper;
            long long projected = std::pow(local_upper, non_zero_columns);
            for (int i = prefix; i < ROW_ARITY - suffix - 1; i++) {
                long long upper = local_upper;
                if (labs(global_upper - projected / local_upper * (local_upper + 1)) < labs(global_upper - projected)) {
                    upper++;
                    projected = projected / local_upper * (local_upper + 1);
                }
                acc *= upper;
                rem /= upper;
                dist[i] = std::uniform_int_distribution<std::mt19937::result_type>(0, upper - 1);
            }

            if (labs(global_upper - acc * (rem + 1)) < labs(global_upper - acc * rem)) {
                rem++;
            }

            dist[ROW_ARITY - suffix - 1] = std::uniform_int_distribution<std::mt19937::result_type>(0, rem - 1);
        }

        ApproximateDuplicateGenerator(int num_rows, double percent_unique, int prefix = 0, int suffix = 0,
                                      unsigned long seed = -1)
                : ApproximateDuplicateGenerator(num_rows, (int) (num_rows * percent_unique), prefix, suffix, seed) {};

        Generator *clone() const override {
            return new ApproximateDuplicateGenerator(num_rows, output_size, prefix, suffix, seed);
        }

        Row *next() override {
            Iterator::next();
            if (count >= num_rows) {
                return nullptr;
            }
            count++;
            row.tid++;
            for (int i = prefix; i < ROW_ARITY - suffix; i++) {
                row.columns[i] = dist[i](rng);
            }
            return &row;
        }

    private:
        int num_rows;
        int output_size;
        int prefix;
        int suffix;
        Row row;
        int count;

        std::mt19937 rng;
        std::uniform_int_distribution<std::mt19937::result_type> dist[ROW_ARITY];
        unsigned long seed;
    };
}
