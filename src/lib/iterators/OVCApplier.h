#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class OVCApplier : public UnaryIterator {
    public:
        explicit OVCApplier(Iterator *input) : UnaryIterator(input), prev_({0}) {
            assert(input->outputIsSorted());
            output_is_sorted = true;
            output_has_ovc = true;
            output_is_unique = input->outputIsUnique();
        };

        Row *next() override {
            Row *row = input_->next();
            if (row == nullptr) {
                return nullptr;
            }
            prev_.less(*row, row->key);
            prev_ = *row;
            return row;
        };

        void open() override {
            Iterator::open();
            input_->open();
        };

        void free() override {
            Iterator::free();
            input_->free();
        };

        void close() override {
            Iterator::close();
            input_->close();
        };

    private:
        Row prev_;
    };
}