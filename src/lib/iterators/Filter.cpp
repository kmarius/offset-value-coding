#include "Filter.h"

Filter::Filter(Predicate *const predicate, Iterator *const input)
        : predicate_(predicate), input_(input) {
}

Filter::~Filter() {
    assert(status == Closed);
    delete input_;
}

void Filter::open() {
    assert(status == Unopened);
    status = Opened;
    input_->open();
}

void Filter::close() {
    assert(status == Opened);
    status = Closed;
    input_->close();
}

Row *Filter::next() {
    assert(status == Opened);

    Row *row;
    for (; (row = input_->next());) {
        if (predicate_(row)) {
            return row;
        };
        input_->free();
    }
    return nullptr;
}

void Filter::free() {
    input_->free();
}
