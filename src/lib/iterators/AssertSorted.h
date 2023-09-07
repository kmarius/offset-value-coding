#pragma once

#include "Iterator.h"
#include "lib/log.h"

namespace ovc::iterators {

/**
 * Checks if the input_ is ascending and therefore sorted.
 */
    class AssertSorted : public UnaryIterator {
    public:
        explicit AssertSorted(Iterator *iterator) : UnaryIterator(iterator),
                                                    is_sorted(true), count_(0), prev({0}) {}

        void open() override {
            Iterator::open();
            input_->open();
        };

        Row *next() override {
            Iterator::next();
            Row *row = input_->next();
            if (row == nullptr) {
                return nullptr;
            }
            if (is_sorted && cmp(*row, prev) < 0) {
                log_error("input not sorted at %d: prev: %s", count_ + 1, prev.c_str());
                log_error("                        cur: %s", row->c_str());
                is_sorted = false;
            }
            prev = *row;
            count_++;
            return row;
        };

        void free() override {
            Iterator::free();
            input_->free();
        };

        void close() override {
            Iterator::close();
            input_->close();

        };

        bool isSorted() const {
            return is_sorted;
        };

        size_t count() const {
            return count_;
        };

    private:
        Row prev;
        bool is_sorted;
        size_t count_;
        RowCmp cmp;
    };
}