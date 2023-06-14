
#include "HashDedup.h"

HashDedup::HashDedup(Iterator *input) : input_(input), hashSet(1 << RUN_IDX_BITS) {
}

HashDedup::~HashDedup() {
    assert(status == Closed);
    delete input_;
}

void HashDedup::open() {
    Iterator::open();
    input_->open();

    // fill and probe external hashmap
    for (Row *row = input_->next(); row; row = input_->next()) {
        row->setHash();
        hashSet.put(row);
    }
}

void HashDedup::close() {
    Iterator::close();
    input_->close();
}

Row *HashDedup::next() {
    Iterator::next();
    return hashSet.next();
}