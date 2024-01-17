#pragma once

#include "Iterator.h"
#include "Sort.h"
#include "lib/aggregates.h"

namespace ovc::iterators {

    template<bool USE_OVC, typename Aggregate, typename Compare>
    class InSortGroupByBase : public UnaryIterator {
    public:
        explicit InSortGroupByBase(Iterator *input, int groupColumns, const Aggregate &agg, const Compare &cmp)
                : UnaryIterator(input), sorter(&stats, cmp, agg), count(0) {
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
        Sorter<false, USE_OVC, Compare, Aggregate, true> sorter;
        unsigned long count;
    };

    template<typename Aggregate>
    class InSortGroupBy : public InSortGroupByBase<true, Aggregate, RowCmpPrefixOVC> {
    public:
        InSortGroupBy(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<true, Aggregate, RowCmpPrefixOVC>(input, groupColumns, agg,
                                                                      RowCmpPrefixOVC(groupColumns, &this->stats)) {};
    };

    template<typename Aggregate>
    class InSortGroupByNoOvc : public InSortGroupByBase<false, Aggregate, CmpPrefixNoOVC> {
    public:
        InSortGroupByNoOvc(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<false, Aggregate, CmpPrefixNoOVC>(input, groupColumns, agg,
                                                                      CmpPrefixNoOVC(groupColumns,
                                                                                     &this->stats)) {};
    };
}