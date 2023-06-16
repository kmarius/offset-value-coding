#include "AssertSortedUnique.h"
#include "lib/log.h"

AssertSortedUnique::AssertSortedUnique(Iterator *input) : UnaryIterator(input),
                                                          prev({0}), has_prev(false), is_sorted(true), num_rows(0) {}

Row *AssertSortedUnique::next() {
    Row *row = UnaryIterator::next();
    if (row == nullptr) {
        return nullptr;
    }

    if (has_prev && is_sorted && !prev.less(*row)) {
        log_error("input_ not is_sorted: prev: %s", prev.c_str());
        log_error("                      cur: %s", row->c_str());
        is_sorted = false;
    }
    has_prev = true;

    prev = *row;
    num_rows++;
    return row;
}

bool AssertSortedUnique::isSortedAndUnique() const {
    return is_sorted;
}

size_t AssertSortedUnique::count() const {
    return num_rows;
}