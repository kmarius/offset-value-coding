#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class AssertCorrectOVC : public UnaryIterator {
    public:
        explicit AssertCorrectOVC(Iterator *input, int prefix = ROW_ARITY)
                : UnaryIterator(input), prev(), prefix(prefix), correct(true), has_prev(false) {
            assert(input->outputIsSorted());
            output_is_sorted = true;
            output_has_ovc = true;
            output_is_unique = input->outputIsUnique();
        };

        Row *next() override {
            Row *row = input_->next();
            if (row == nullptr) {
                return nullptr;
            }
            if (has_prev) {
                long ovc = row->calcOVC(prev, prefix);
                if (ovc != row->key) {
                    char buf[128];
                    log_error("wrong OVC in row %s w.r.t. base %s", row->c_str(), prev.c_str(buf));
                    correct = false;
                }
            }
            prev = *row;
            has_prev = true;
            return row;
        };

        void open() override {
            Iterator::open();
            input_->open();
        };

        void free() override {
            Iterator::free();
            input_->free();
        };

        void close() override {
            Iterator::close();
            input_->close();
        };

        bool isCorrect() const {
            return correct;
        }

    private:
        Row prev;
        int prefix;
        bool has_prev;
        bool correct;
    };
}