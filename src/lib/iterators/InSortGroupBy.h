#pragma once

#include "Iterator.h"
#include "Sort.h"
#include "lib/aggregates.h"
#include "lib/comparators.h"

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
    class InSortGroupByOVC : public InSortGroupByBase<true, Aggregate, CmpPrefixOVC> {
    public:
        InSortGroupByOVC(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<true, Aggregate, CmpPrefixOVC>(input, groupColumns, agg,
                                                                   CmpPrefixOVC(groupColumns, &this->stats)) {};
    };

    template<typename Aggregate>
    class InSortGroupBy : public InSortGroupByBase<false, Aggregate, CmpPrefix> {
    public:
        InSortGroupBy(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InSortGroupByBase<false, Aggregate, CmpPrefix>(input, groupColumns, agg,
                                                                 CmpPrefix(groupColumns,
                                                                           &this->stats)) {};
    };
}