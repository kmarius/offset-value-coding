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
    if (prev) {
        if (sorted && row->less(*prev)) {
            fail0 = *prev;
            fail1 = *row;
            first_fail = num_rows;
            sorted = false;
        }
    }
    prev_buf = *row;
    prev = &prev_buf;
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

AssertSorted::AssertSorted(Iterator *iterator) : input(iterator), prev(nullptr), sorted(true), num_rows(0) {}

AssertSorted::~AssertSorted() {
    assert(status == Closed);
    delete input;
}

bool AssertSorted::isSorted() const {
    return sorted;
}

size_t AssertSorted::count() const {
    return num_rows;
}
