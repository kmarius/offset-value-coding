#pragma once

#include "Iterator.h"
#include "lib/log.h"
#include "lib/comparators.h"

namespace ovc::iterators {

/**
 * Checks if the input is ascending and therefore sorted.
 */
    template<typename Compare = comparators::Cmp>
    class AssertSorted : public UnaryIterator {
    public:
        explicit AssertSorted(Iterator *iterator, const Compare &cmp = Compare()) : UnaryIterator(iterator),
                                                                                    is_sorted(true), count(0),
                                                                                    prev({0}), cmp(cmp) {}

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
            if (is_sorted) {
                if (cmp.raw(*row, prev) < 0) {
                    log_error("input not sorted at %d: prev: %s", count, prev.c_str());
                    log_error("                        cur: %s", row->c_str());
                    is_sorted = false;
                }
                if constexpr (cmp.USES_OVC) {
                    if (count == 0) {
                        auto wanted = cmp.makeInitialOVC(*row);
                        if (row->key != wanted) {
                            log_error("wrong OVC at %d: prev: %s", count, prev.c_str());
                            log_error("                  cur: %s", row->c_str());
                            log_error("wanted=%lu@%lu, found=%lu@%lu", OVC_FMT(wanted), OVC_FMT(row->key));
                            is_sorted = false;
                        }
                    } else {
                        unsigned long ovc;
                        cmp.raw(*row, prev, &ovc);
                        if (ovc != row->key) {
                            log_error("wrong OVC at %d: prev: %s", count, prev.c_str());
                            log_error("                  cur: %s", row->c_str());
                            log_error("wanted=%lu@%lu, found=%lu@%lu", OVC_FMT(ovc), OVC_FMT(row->key));
                            is_sorted = false;
                        }
                    }
                }
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
        Compare cmp;
        Row prev;
        bool is_sorted;
        size_t count;
    };
}