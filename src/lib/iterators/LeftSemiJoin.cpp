#include "LeftSemiJoin.h"
#include "lib/log.h"

namespace ovc::iterators {
    LeftSemiJoin::LeftSemiJoin(Iterator *left, Iterator *right, unsigned join_columns)
            : BinaryIterator(left, right), join_columns(join_columns) {
        assert(left->outputIsSorted());
        assert(right->outputIsSorted());
    }

    void LeftSemiJoin::open() {
        Iterator::open();
        left->open();
        right->open();
        Row *row = right->next();
        if (row) {
            buffer.push_back(*row);
            right->free();
        }
    }

    Row *LeftSemiJoin::next() {
        Iterator::next();
        if (buffer.empty()) {
            return nullptr;
        }
        for (;;) {
            Row *row_left = left->next();
            if (row_left == nullptr) {
                buffer.clear();
                return nullptr;
            }

            OVC ovc;

            // while buffered right is smaller (per join_columns prefix), replace it with next
            for (; buffer[0].cmp_prefix(*row_left, ovc, 0, join_columns, nullptr); ) {
                Row *row_right = right->next();
                if (row_right == nullptr) {
                    buffer.clear();
                    return nullptr;
                }
                buffer[0] = *row_right;
                right->free();
            }

            // if left is now smaller, continue loop
            if (row_left->cmp_prefix(buffer[0], ovc, 0, join_columns, nullptr)) {
                left->free();
                continue;
            }

            char buf[128];
            //log_info("joined: %s and %s", row_left->c_str(), buffer[0].c_str(buf));

            return row_left;
        }
    }

    void LeftSemiJoin::free() {
        Iterator::free();
        left->free();
    }

    void LeftSemiJoin::close() {
        Iterator::close();
        left->close();
        right->close();
    }
}