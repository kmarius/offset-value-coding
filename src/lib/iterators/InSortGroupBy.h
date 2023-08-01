#pragma once

#include "Iterator.h"
#include "Sort.h"
#include "lib/aggregates.h"

namespace ovc::iterators {

    template<bool USE_OVC, typename Aggregate>
    class InSortGroupByBase : public UnaryIterator {
    public:
        explicit InSortGroupByBase(Iterator *input, int groupColumns, const Aggregate &agg)
                : UnaryIterator(input), sorter(RowCmpPrefix(groupColumns), agg), count(0) {
            output_has_ovc = false;
            output_is_sorted = false;
            output_is_unique = true;
        }

        void open() {
            Iterator::open();
            input_->open();
            sorter.consume(input_);
            input_->close();
        }

        Row *next() {
            if (sorter.queue.isEmpty()) {
                return nullptr;
            }
            count++;
            return sorter.next();
        }

        void close() {
            Iterator::close();
            sorter.cleanup();
        }

        struct iterator_stats &getStats() {
            return sorter.queue.getStats();
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Sorter<false, USE_OVC, RowCmpPrefix, Aggregate, true> sorter;
        unsigned long count;
    };

    template<typename Aggregate>
    class InSortGroupBy : public InSortGroupByBase<true, Aggregate> {
    public:
        InSortGroupBy(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<true, Aggregate>(input, groupColumns, agg) {};
    };

    template<typename Aggregate>
    class InSortGroupByNoOvc : public InSortGroupByBase<false, Aggregate> {
    public:
        InSortGroupByNoOvc(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<false, Aggregate>(input, groupColumns, agg) {};
    };
}