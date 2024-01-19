#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    template<bool USE_OVC, typename Aggregate>
    class InStreamGroupByBase : public UnaryIterator {
    public:
        explicit InStreamGroupByBase(Iterator *input, int group_columns, const Aggregate &agg = Aggregate());

        void open() override;

        Row *next() override;

        void close() override {
            Iterator::close();
            input->close();
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Aggregate agg;
        Row acc_buf;   // holds the first row of the group
        Row output_buf;  // holds the Row we return in next
        bool empty;
        int group_columns;
        unsigned long count;
    };

    template<typename Aggregate>
    class InStreamGroupByOVC : public InStreamGroupByBase<true, Aggregate> {
    public:
        InStreamGroupByOVC(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InStreamGroupByBase<true, Aggregate>(input, groupColumns, agg) {};
    };

    template<typename Aggregate>
    class InStreamGroupBy : public InStreamGroupByBase<false, Aggregate> {
    public:
        InStreamGroupBy(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InStreamGroupByBase<false, Aggregate>(input, groupColumns, agg) {};
    };
}

#include "InStreamGroupBy.ipp"