#include "Dedup.h"
#include "lib/log.h"

Dedup::Dedup(Iterator *const input)
        : input_(input), num_dupes(0) {
}

Dedup::~Dedup() {
    assert(status == Closed);
    delete input_;
}

void Dedup::open() {
    assert(status == Unopened);
    status = Opened;
    input_->open();
}

void Dedup::close() {
    assert(status == Opened);
    status = Closed;
    input_->close();
}

Row *Dedup::next() {
    assert(status == Opened);

    Row *row;
    for (; (row = input_->next());) {
        if (row->key != 0) {
            return row;
        }
        input_->free();
        num_dupes++;
    }
    return nullptr;
}

void Dedup::free() {
    input_->free();
}