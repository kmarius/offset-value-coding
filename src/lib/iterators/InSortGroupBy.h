#pragma once

#include "Iterator.h"
#include "Sort.h"
#include "lib/aggregates.h"

namespace ovc::iterators {

    template<bool USE_OVC, typename Aggregate>
    class InSortGroupByBase : public UnaryIterator {
    public:
        explicit InSortGroupByBase(Iterator *input, int groupColumns, const Aggregate &agg)
                : UnaryIterator(input), sorter(&stats, RowCmpPrefixNoOVC(groupColumns), agg), count(0) {
        }

        void open() {
            Iterator::open();
            input->open();
            sorter.consume(input);
            input->close();
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

        unsigned long getCount() const {
            return count;
        }

    private:
        Sorter<false, USE_OVC, RowCmpPrefixNoOVC, Aggregate, true> sorter;
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