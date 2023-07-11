#include "Dedup.h"
#include "lib/log.h"

template<bool USE_OVC>
DedupBase<USE_OVC>::DedupBase(Iterator *const input)
        : UnaryIterator(input), num_dupes(0), has_prev(false), prev({0}) {
    assert(input->outputIsSorted());
    assert(!USE_OVC || input->outputHasOVC());
    output_is_unique = true;
    output_is_sorted = true;
}

template<bool USE_OVC>
Row *DedupBase<USE_OVC>::next() {
    for (Row *row; (row = UnaryIterator::next()); UnaryIterator::free()) {
        if (USE_OVC) {
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
class DedupBase<true>;

template
class DedupBase<false>;