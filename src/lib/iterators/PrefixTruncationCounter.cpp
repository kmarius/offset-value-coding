#include "PrefixTruncationCounter.h"

PrefixTruncationCounter::PrefixTruncationCounter(Iterator *input) : UnaryIterator(input),
                                                                    count(0), prev({0}), has_prev(false) {
    assert(input->outputIsSorted());
}

Row *PrefixTruncationCounter::next() {
    Row *row = UnaryIterator::next();
    if (row == nullptr) {
        return nullptr;
    }
    if (has_prev) {
        for (int i = 0; i < ROW_ARITY; i++) {
            count++;
            if (row->columns[i] != prev.columns[i]) {
                break;
            }
        }
    } else {
        has_prev = true;
    }
    prev = *row;
    return row;
}

unsigned long PrefixTruncationCounter::getColumnComparisons() const {
    return count;
}
