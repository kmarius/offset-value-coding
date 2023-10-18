#include "InStreamDistinct.h"
#include "lib/log.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    InStreamDistinctBase<USE_OVC>::InStreamDistinctBase(Iterator *const input)
            : UnaryIterator(input), num_dupes(0), has_prev(false), prev({0}) {
        assert(input->outputIsSorted());
        assert(!USE_OVC || input->outputHasOVC());
    }

    template<bool USE_OVC>
    Row *InStreamDistinctBase<USE_OVC>::next() {
        for (Row *row; (row = input->next()); input->free()) {
            if constexpr (USE_OVC) {
                // TODO: we can repair OVCs here
                if (row->key != 0) {
                    return row;
                }
            } else {
                if (!has_prev || !row->equals(prev)) {
                    prev = *row;
                    has_prev = true;
                    return row;
                }
            }
            num_dupes++;
        }
        return nullptr;
    }

    template
    class InStreamDistinctBase<true>;

    template
    class InStreamDistinctBase<false>;
}