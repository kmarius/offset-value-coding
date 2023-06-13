#include "AssertSortedUnique.h"
#include "lib/log.h"

void AssertSortedUnique::open() {
    assert(status == Unopened);
    status = Opened;
    input->open();
}

Row *AssertSortedUnique::next() {
    assert(status == Opened);
    Row *row = input->next();
    if (row == nullptr) {
        return nullptr;
    }

    if (has_prev && sorted && !prev.less(*row)) {
        log_error("input not is_sorted: prev: %s", prev.c_str());
        log_error("                      cur: %s", row->c_str());
        sorted = false;
    }
    has_prev = true;

    prev = *row;
    num_rows++;
    return row;
}

void AssertSortedUnique::free() {
    input->free();
}

void AssertSortedUnique::close() {
    assert(status == Opened);
    status = Closed;
    input->close();
}

AssertSortedUnique::AssertSortedUnique(Iterator *iterator) : input(iterator), prev({0}), has_prev(false), sorted(true),
                                                             num_rows(0) {}

AssertSortedUnique::~AssertSortedUnique() {
    assert(status == Closed);
    delete input;
}

bool AssertSortedUnique::isSortedAndUnique() const {
    return sorted;
}

size_t AssertSortedUnique::count() const {
    return num_rows;
}
