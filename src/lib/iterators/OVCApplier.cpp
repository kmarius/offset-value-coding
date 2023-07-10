#include "OVCApplier.h"

OVCApplier::OVCApplier(Iterator *input) : UnaryIterator(input), prev_({0}) {
    assert(input->outputIsSorted());
}

Row *OVCApplier::next() {
    Row *row = UnaryIterator::next();
    if (row == nullptr) {
        return nullptr;
    }
    prev_.less(*row, row->key);
    prev_ = *row;
    return row;
}