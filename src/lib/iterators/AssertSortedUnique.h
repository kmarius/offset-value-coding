#pragma once

#include "Iterator.h"
#include "lib/log.h"
#include "lib/comparators.h"

namespace ovc::iterators {

/**
 * Checks if the input is strictly ascending and therefore sorted and uniqe.
 */
    class AssertSortedUnique : public UnaryIterator {
    public:
        explicit AssertSortedUnique(Iterator *input) : UnaryIterator(input),
                                                       prev({0}), has_prev(false), is_sorted(true),
                                                       count_(0), cmp() {};

        void open() override {
            Iterator::open();
            input->open();
        };

        Row *next() override {
            Iterator::next();
            Row *row = input->next();
            if (row == nullptr) {
                return nullptr;
            }

            if (has_prev && is_sorted && cmp(prev, *row) >= 0) {
                log_error("input not is_sorted: prev: %s", prev.c_str());
                log_error("                      cur:  %s", row->c_str());
                is_sorted = false;
            }
            has_prev = true;

            prev = *row;
            count_++;
            return row;
        };

        void free() override {
            Iterator::free();
            input->free();
        };

        void close() override {
            Iterator::close();
            input->close();
        };

        size_t count() const {
            return count_;
        };

        bool isSortedAndUnique() const {
            return is_sorted;
        };

    private:
        Row prev;
        bool has_prev;
        bool is_sorted;
        size_t count_;
        comparators::Cmp cmp;
    };
}