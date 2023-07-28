#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    class InStreamGroupByBase : public UnaryIterator {
    public:
        explicit InStreamGroupByBase(Iterator *input, int group_columns);

        void open() override;

        Row *next() override;

        // overwritten, because we own the rows returned by next and the base method calls free on the input
        void free() override {};

        void close() override {
            Iterator::close();
            input_->close();
        }

    private:
        Row input_buf;   // holds the first row of the next group
        Row output_buf;  // holds the Row we return in next
        bool empty;
        int group_columns;
    };

    typedef InStreamGroupByBase<true> InStreamGroupBy;
    typedef InStreamGroupByBase<true> InStreamGroupByNoOvc;
}