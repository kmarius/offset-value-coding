#include "Filter.h"

namespace ovc::iterators {

    Filter::Filter(Iterator *input, Predicate *predicate)
            : UnaryIterator(input), predicate_(predicate) {
        output_is_sorted = input->outputIsSorted();
        output_is_hashed = input->outputIsHashed();
        output_is_unique = input->outputIsUnique();
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
}