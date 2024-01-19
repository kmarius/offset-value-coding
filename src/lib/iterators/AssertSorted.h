#pragma once

#include "Iterator.h"
#include "lib/log.h"
#include "lib/comparators.h"

namespace ovc::iterators {

/**
 * Checks if the input is ascending and therefore sorted.
 */
    class AssertSorted : public UnaryIterator {
    public:
        explicit AssertSorted(Iterator *iterator) : UnaryIterator(iterator),
                                                    is_sorted(true), count(0), prev({0}) {}

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
            if (is_sorted && cmp(*row, prev) < 0) {
                log_error("input not sorted at %d: prev: %s", count + 1, prev.c_str());
                log_error("                        cur: %s", row->c_str());
                is_sorted = false;
            }
            prev = *row;
            count++;
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

        bool isSorted() const {
            return is_sorted;
        };

        size_t getCount() const {
            return count;
        };

    private:
        Row prev;
        bool is_sorted;
        size_t count;
        comparators::Cmp cmp;
    };
}