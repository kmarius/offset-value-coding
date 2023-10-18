#include "PrefixTruncationCounter.h"

namespace ovc::iterators {

    PrefixTruncationCounter::PrefixTruncationCounter(Iterator *input) : UnaryIterator(input),
                                                                        count(0), prev({0}), has_prev(false) {
        assert(input->outputIsSorted());
    }

    Row *PrefixTruncationCounter::next() {
        Row *row = input->next();
        if (row == nullptr) {
            return nullptr;
        }
        if (has_prev) {
            for (int i = 0; i < ROW_ARITY; i++) {
                stats.column_comparisons++;
                count++;
                if (row->columns[i] != prev.columns[i]) {
                    break;
                }
            }
        } else {
            has_prev = true;
        }
        prev = *row;
        return row;
    }

    unsigned long PrefixTruncationCounter::getColumnComparisons() const {
        return stats.column_comparisons;
    }

    void PrefixTruncationCounter::open() {
        Iterator::open();
        input->open();
    }

    void PrefixTruncationCounter::free() {
        Iterator::free();
        input->next();
    }

    void PrefixTruncationCounter::close() {
        Iterator::close();
        input->close();
    }
}