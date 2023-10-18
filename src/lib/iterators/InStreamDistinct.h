#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    class InStreamDistinctBase : public UnaryIterator {
    public:
        explicit InStreamDistinctBase(Iterator *input);

        void open() override {
            Iterator::open();
            input->open();
        }

        Row *next() override;

        void free() override {
            Iterator::free();
            input->free();
        }

        void close() override {
            Iterator::close();
            input->close();
        }

        size_t num_dupes;

    private:
        Row prev;
        bool has_prev;
    };

    typedef InStreamDistinctBase<true> InStreamDistinct;
    typedef InStreamDistinctBase<false> InStreamDistinctNoOvc;
}