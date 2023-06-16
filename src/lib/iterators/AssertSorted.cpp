#include "AssertSorted.h"
#include "lib/log.h"

AssertSorted::AssertSorted(Iterator *iterator) : UnaryIterator(iterator),
                                                 is_sorted(true), num_rows(0), prev({0}) {}

Row *AssertSorted::next() {
    Row *row = UnaryIterator::next();
    if (row == nullptr) {
        return nullptr;
    }
    if (is_sorted && row->less(prev)) {
        log_error("input not sorted at %d: prev: %s", num_rows + 1, prev.c_str());
        log_error("                         cur: %s", row->c_str());
        is_sorted = false;
    }
    prev = *row;
    num_rows++;
    return row;
}

bool AssertSorted::isSorted() const {
    return is_sorted;
}

size_t AssertSorted::count() const {
    return num_rows;
}
