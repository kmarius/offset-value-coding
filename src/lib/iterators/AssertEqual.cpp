#include "AssertEqual.h"
#include "lib/log.h"

namespace ovc::iterators {

    AssertEqual::AssertEqual(Iterator *left, Iterator *right) : BinaryIterator(left, right), equal(true), count(0) {}

    void AssertEqual::open() {
        Iterator::open();
        left->open();
        right->open();
    }

    Row *AssertEqual::next() {
        Iterator::next();
        Row *left_row = left->next();
        Row *right_row = right->next();
        if (left_row == nullptr && right_row == nullptr) {
            return nullptr;
        }
        if (left_row == nullptr) {
            equal = false;
            log_error("AssertEqual: right_row input is longer");
            log_error("right_row: %s", right_row->c_str());
            right->free();
            return nullptr;
        }
        if (right_row == nullptr) {
            equal = false;
            log_error("AssertEqual: left_row input is longer");
            log_error("left_row: %s", left_row->c_str());
            left->free();
            return nullptr;
        }
        if (!left_row->equals(*right_row)) {
            equal = false;
            log_error("AssertEqual at row %lu:", count);
            log_error("left_row: %s", left_row->c_str());
            log_error("right_row: %s", right_row->c_str());
            return nullptr;
        }

        count++;
        return left_row;
    }

    void AssertEqual::free() {
        Iterator::free();
        left->free();
        right->free();
    }

    void AssertEqual::close() {
        Iterator::close();
        left->close();
        right->close();
    }
}