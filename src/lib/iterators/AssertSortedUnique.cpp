#include "AssertSortedUnique.h"
#include "lib/log.h"

AssertSortedUnique::AssertSortedUnique(Iterator *iterator) : input(iterator), prev({0}), has_prev(false), is_sorted(true),
                                                             num_rows(0) {}

AssertSortedUnique::~AssertSortedUnique() {
    delete input;
}

void AssertSortedUnique::open() {
    Iterator::open();
    input->open();
}

Row *AssertSortedUnique::next() {
    Iterator::next();
    Row *row = input->next();
    if (row == nullptr) {
        return nullptr;
    }

    if (has_prev && is_sorted && !prev.less(*row)) {
        log_error("input not is_sorted: prev: %s", prev.c_str());
        log_error("                      cur: %s", row->c_str());
        is_sorted = false;
    }
    has_prev = true;

    prev = *row;
    num_rows++;
    return row;
}

void AssertSortedUnique::free() {
    Iterator::free();
    input->free();
}

void AssertSortedUnique::close() {
    Iterator::close();
    input->close();
}

bool AssertSortedUnique::isSortedAndUnique() const {
    return is_sorted;
}

size_t AssertSortedUnique::count() const {
    return num_rows;
}