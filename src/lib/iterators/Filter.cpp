#include "Filter.h"

Filter::Filter(Predicate *const predicate, Iterator *const input)
        : predicate_(predicate), input_(input) {
}

Filter::~Filter() {
    delete input_;
}

void Filter::open() {
    Iterator::open();
    input_->open();
}

void Filter::close() {
    Iterator::close();
    input_->close();
}

Row *Filter::next() {
    Iterator::next();

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