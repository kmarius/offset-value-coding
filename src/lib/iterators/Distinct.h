#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    class DistinctBase : public UnaryIterator {
    public:
        explicit DistinctBase(Iterator *input);

        void open() override {
            Iterator::open();
            input_->open();
        }

        Row *next() override;

        void free() override {
            Iterator::free();
            input_->free();
        }

        void close() override {
            Iterator::close();
            input_->close();
        }

        size_t num_dupes;

    private:
        Row prev;
        bool has_prev;
    };

    typedef DistinctBase<true> Distinct;
    typedef DistinctBase<false> DistinctNoOvc;
}