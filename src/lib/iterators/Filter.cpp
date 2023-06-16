#include "Filter.h"

Filter::Filter(Iterator *input, Predicate *predicate)
        : UnaryIterator(input), predicate_(predicate) {
}

Row *Filter::next() {
    for (Row *row; (row = UnaryIterator::next());) {
        if (predicate_(row)) {
            return row;
        };
        UnaryIterator::free();
    }
    return nullptr;
}

bool Filter::outputIsHashed() {
    return input_->outputIsHashed();
}

bool Filter::outputIsSorted() {
    return input_->outputIsSorted();
}

bool Filter::outputHasOVC() {
    return input_->outputHasOVC();
}