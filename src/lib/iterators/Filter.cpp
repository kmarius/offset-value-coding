#include "Filter.h"

namespace ovc::iterators {

    Filter::Filter(Iterator *input, Predicate *predicate)
            : UnaryIterator(input), predicate_(predicate) {
        output_is_sorted = input->outputIsSorted();
        output_is_hashed = input->outputIsHashed();
        output_is_unique = input->outputIsUnique();
    }

    Row *Filter::next() {
        Iterator::next();
        for (Row *row; (row = input_->next()); input_->free()) {
            if (predicate_(row)) {
                return row;
            };
        }
        return nullptr;
    }

    void Filter::open() {
        Iterator::open();
        input_->open();
    }

    void Filter::free() {
        Iterator::free();
        input_->free();
    }

    void Filter::close() {
        Iterator::close();
        input_->close();
    }
}