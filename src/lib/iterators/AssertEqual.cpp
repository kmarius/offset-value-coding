#include "AssertEqual.h"
#include "lib/log.h"

void AssertEqual::open() {
    left_->open();
    right_->open();
}

Row *AssertEqual::next() {
    Row *left = left_->next();
    Row *right = right_->next();
    if (left == nullptr && right == nullptr) {
        return nullptr;
    }
    if (left == nullptr) {
        equal = false;
        right_->free();
        log_error("AssertEqual: right input is longer");
        return nullptr;
    }
    if (right == nullptr) {
        equal = false;
        log_error("AssertEqual: left input is longer");
        left_->free();
        return nullptr;
    }
    if (!left->equals(*right)) {
        equal = false;
        log_error("AssertEqual: ");
        log_error("%s", left->c_str());
        log_error("%s", right->c_str());
        return nullptr;
    }

    return left;
}

void AssertEqual::free() {
    left_->free();
    right_->free();
}

void AssertEqual::close() {
    left_->close();
    right_->close();
}

AssertEqual::AssertEqual(Iterator *left, Iterator *right) : left_(left), right_(right), equal(true) {
}

AssertEqual::~AssertEqual() {
    delete left_;
    delete right_;
}
