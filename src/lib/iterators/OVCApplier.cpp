#include "OVCApplier.h"

OVCApplier::OVCApplier(Iterator *input) : UnaryIterator(input), prev_({0}) {
    assert(input->outputIsSorted());
    output_is_sorted = true;
    output_has_ovc = true;
    output_is_unique = input->outputIsUnique();
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