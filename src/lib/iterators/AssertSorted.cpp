#include "AssertSorted.h"
#include "lib/log.h"

void AssertSorted::open() {
    assert(status == Unopened);
    status = Opened;
    input->open();
}

Row *AssertSorted::next() {
    assert(status == Opened);
    Row *row = input->next();
    if (row == nullptr) {
        return nullptr;
    }
    if (is_sorted && row->less(prev)) {
        log_error("input not is_sorted: prev: %s", prev.c_str());
        log_error("                      cur: %s", row->c_str());
        is_sorted = false;
    }
    prev = *row;
    num_rows++;
    return row;
}

void AssertSorted::free() {
    input->free();
}

void AssertSorted::close() {
    assert(status == Opened);
    status = Closed;
    input->close();
}

AssertSorted::AssertSorted(Iterator *iterator) : input(iterator), is_sorted(true), num_rows(0), prev({0}) {}

AssertSorted::~AssertSorted() {
    assert(status == Closed);
    delete input;
}

bool AssertSorted::isSorted() const {
    return is_sorted;
}

size_t AssertSorted::count() const {
    return num_rows;
}
