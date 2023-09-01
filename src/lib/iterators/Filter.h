#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    /*
     * If USE_OVC is enabled, the filter will repair offset value codes using the maximum formula.
     */
    template<bool USE_OVC = false>
    class Filter : public UnaryIterator {
    public :
        typedef bool (Predicate)(const Row *row);

        Filter(Iterator *input, Predicate *pred) : UnaryIterator(input), pred(pred), max_ovc(0) {
            output_is_sorted = input->outputIsSorted();
            output_is_hashed = input->outputIsHashed();
            output_is_unique = input->outputIsUnique();
        };

        void open() override {
            Iterator::open();
            input_->open();
        };

        Row *next() override {
            Iterator::next();
            for (Row *row; (row = input_->next()); input_->free()) {
                if constexpr (USE_OVC) {
                    if (pred(row)) {
                        if (max_ovc > row->key) {
                            row->key = max_ovc;
                        }
                        max_ovc = 0;
                        return row;
                    }
                    if (row->key > max_ovc) {
                        max_ovc = row->key;
                    }
                } else {
                    if (pred(row)) {
                        return row;
                    }
                }
            }
            return nullptr;
        };

        void free() override {
            Iterator::free();
            input_->free();
        };

        void close() override {
            Iterator::close();
            input_->close();
        };

    private :
        OVC max_ovc;
        Predicate *const pred;
    };
}