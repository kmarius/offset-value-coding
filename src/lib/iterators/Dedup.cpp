#include "Dedup.h"
#include "lib/log.h"

Dedup::Dedup(Iterator *const input)
        : UnaryIterator(input), num_dupes(0), has_prev(false), prev({0}) {
    assert(input->outputIsSorted());
}

Row *Dedup::next() {
    for (Row *row; (row = UnaryIterator::next()); UnaryIterator::free()) {
#ifdef PRIORITYQUEUE_NO_USE_OVC
        if (!has_prev || !row->equals(prev)) {
            prev = *row;
            has_prev = true;
            return row;
        }
#else
        if (row->key != 0) {
            return row;
        }
#endif
        num_dupes++;
    }
    return nullptr;
}