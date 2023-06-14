#include "Dedup.h"
#include "lib/log.h"

Dedup::Dedup(Iterator *const input)
        : input_(input), num_dupes(0), has_prev(false), prev({0}) {
    assert(input->outputIsSorted());
}

Dedup::~Dedup() {
    delete input_;
}

void Dedup::open() {
    Iterator::open();
    input_->open();
}

void Dedup::close() {
    Iterator::close();
    input_->close();
}

Row *Dedup::next() {
    Iterator::next();

    for (Row *row; (row = input_->next());) {
#ifdef PRIORITYQUEUE_NO_USE_OVC
        if (!has_prev || !row->equals(prev)) {
            prev = *row;
            has_prev = true;
            return row;
        }
#else
        if (row->key != 0) {
            return row;
        }
#endif
        input_->free();
        num_dupes++;
    }
    return nullptr;
}

void Dedup::free() {
    Iterator::free();
    input_->free();
}